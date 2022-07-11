"""A module containing my implementation of Java-like streams in Python.
"""

#  Copyright (c) 2022 Dmytro Popov


from __future__ import annotations

import abc
from collections.abc import Collection
from itertools import chain
from numbers import Number
from typing import Type, TypeVar, Generic, Callable, NewType, Iterator, Iterable

T = TypeVar('T')
T1 = TypeVar('T1')
T_col = NewType('T_col', Iterable[T])

Predicate = Callable[[T], bool]
Function = Callable[[T], T1]
Consumer = Callable[[T], None]
Comparator = Callable[[T], 'SupportsLessThan[T]']


class Stream(Generic[T]):
    """A class that represents a Java-like streams.
    Stream objects have intermediate operations that modify the stream itself and operations that consume the stream
    values. After the operations that consume the values, the stream cannot be used anymore, as the iterator becomes
    empty.
    """

    _cls: Type[T]  # a class contained in a stream
    _values: Iterator[T]  # a values iterator

    def __init__(self, cls: Type[T], values: Iterator[T]):
        """Initializes a Stream of type T with values from `values` Iterator[T].

        :param cls: a type contained in a stream
        :param values: values of the stream
        """

        self._cls = cls
        self._values = iter(values)

    @classmethod
    def of(cls, *values: T) -> 'Stream[T]':
        """Initializes a Stream of the given iterable or a sequence of values.
        The stream will have a type of the values.
        If the only argument is a collection, the stream of the values of that collection will be returned.

        :param values: a collection or sequence of values to stream
        :return: Stream of the values of the given iterable
        """

        if len(values) == 1 and isinstance(values[0], Collection):
            return cls(type(values[0][0] if values[0] else T), iter(*values))
        return cls(type(values[0] if values else T), iter(values))

    def collect(self, collection_type: Type[T_col]) -> T_col:
        """Returns the collection of the given type with the values of the stream.

        This is a consuming operation.

        :param collection_type: a type of the collection to collect values in
        :return: a collection of the given type with the values of the stream
        """

        return collection_type(self._values)

    def list(self) -> T_col[list[T]]:
        """Known special case of .collect() method. Returns a list of the values of the stream.

        This is a consuming operation.

        :return: a list of the values of the stream
        """

        return self.collect(list)

    def tuple(self) -> T_col[tuple[T]]:
        """Known special case of .collect() method. Returns a tuple of the values of the stream.

        This is a consuming operation.

        :return: a tuple of the values of the stream
        """

        return self.collect(tuple)

    def to_set(self) -> T_col:
        """Known special case of .to_set() method. Returns a set of the values of the stream.

        This is a consuming operation.

        :return: a set of the values of the stream
        """

        return self.collect(set)

    def map(self, func: Function | Type, map_to: Type[T1] = None) -> 'Stream[T1]':
        """A method to mutate the values of the stream.

        If `func` is a type, the type of the stream will be set to that type.
        Otherwise, the type `map_to` will be used as a stream type, if present.
        In other cases, the type of the stream will not be changed.
        *This is a limitation of Python iterators and typing.*

        This is an intermediate operation.

        :param func: A function to map a stream with
        :param map_to: A type to map a stream to
        :return: Stream[T1]
        """

        self._values = map(func, self._values)
        if isinstance(func, type):
            self._cls = Type[func]
        elif map_to is not None:
            self._cls = map_to

        return self

    def map_to_num(self, func: Callable[[T], NT] = lambda x: x) -> NumStream[NT]:
        """A method to map a stream to the NumStream. If the stream contains Numbers already, the `func` argument
        is not necessary. Otherwise, the `func` function that maps T value to Number is needed.

        This is an intermediate operation.

        :param func: a function that takes T and returns any of the subclasses of the Number abstract class
        :return: NumStream[NT]
        """

        return NumStream.of(*self.map(func).list())

    def filter(self, predicate: Predicate) -> 'Stream[T]':
        """Filters a values of the stream with the given predicate.

        This is an intermediate operation.

        :param predicate: a function that takes a T value and returns a bool
        :return: Stream[T]
        """

        self._values = filter(predicate, self._values)
        return self

    def foreach(self, consumer: Consumer) -> None:
        """Applies a consumer function to each value of the stream.

        This is a consuming operation.

        :param consumer: a function that accepts a T value and returns None
        :return: None
        """

        for elem in self._values:
            consumer(elem)

    def find_first(self, predicate: Predicate) -> T | None:
        """Returns the first element of the stream satisfying the given predicate.

        If no element is found, returns None.

        This is a consuming operation.

        :param predicate: a function that accepts a T value and returns bool
        :return: a value of the stream satisfying the given predicate | None
        """

        for elem in self._values:
            if predicate(elem):
                try:
                    return elem
                finally:
                    del self._values
                    del self
        return None

    def flatmap(self, func: Callable[[T], 'Stream[T1]'] = None) -> 'Stream[T1]':
        """A method that allows to flatten the stream of streams.

        If the function is not given, the Stream.of function if used.

        This is an intermediate operation.

        :param func: a function that takes a T value and returns a stream of T1
        :return: Stream of T1
        """

        if not func:
            func = self.of

        res: Iterator[T1] = iter(())
        for elem in self._values:
            res = chain(res, func(elem)._values)
        self._values = res
        self._cls = Type[T1]
        return self

    def max(self, key: Comparator) -> T:
        """Returns a maximum value of the stream values based on the given key. Returns a T value.

        :param key: a comparing key function that takes a T value and returns a comparable value
        :return: T value
        """

        return max(self._values, key=key)

    def min(self, key: Comparator) -> T:
        """Returns a maximum value of the stream values based on the given key. Returns a T value.

        :param key: a comparing key function that takes a T value and returns a comparable value
        :return: T value
        """

        return max(self._values, key=key)

    def __repr__(self) -> str:
        return f'<Stream[{self._cls.__name__}]>'

    def sorted(self, key: Comparator = None, reverse: bool = False) -> Stream[T]:
        """Returns a sorted Stream of the values of the Stream.

        **All the values of the iterator must be determined and collected to sort the Stream. It means that the iterator
        will be collected to a list, no matter what the size of the stream is.**

        This is an intermediate operation.

        :param key: a comparing key function that takes a T value and returns a comparable value
        :param reverse: if the sorting should be in reverse order
        :return: a sorted Stream of T values
        """

        return self.of(sorted(self._values, key=key, reverse=reverse))

    def count(self) -> int:
        """Returns a quantity of the values in the stream. Consumes the stream.

        This is a consuming operation.

        :return: the length of the stream values iterator
        """

        return len(tuple(self._values))

    def sum(self) -> T:
        """Returns a sum of the values in the stream. Not implemented for Stream.
        See NumStream for implementation.

        :return: a sum of values in the stream
        """
        ...

    def __iter__(self):
        return self

    def __next__(self):
        nxt = next(self._values)
        if nxt is None:
            raise StopIteration
        return nxt


# A generic type extending Number
NT = TypeVar('NT', bound=Number)


class NumStream(Stream[NT]):
    """A Stream of Numbers.
    Has its implementation of the methods sum, max and min.
    """

    _cls: Type[NT]
    _values: Iterator[NT]

    @classmethod
    def of(cls, *values: NT) -> 'NumStream[NT]':
        """Returns a NumStream of the given Numbers.
        If the only value given is a collection, the NumStream will contain the values of that collection.

        :param values: a collection of NT or a sequence of NT values
        :return: NumStream
        """

        if len(values) == 1 and isinstance(values[0], Collection):
            values = values[0]
        if any(not isinstance(v, Number) for v in values):
            raise TypeError('NumStream can only take Numbers')
        return cls(type(values[0] if values else T), values)

    def sum(self) -> NT:
        return sum(self._values)

    def max(self, key=None) -> NT:
        return max(self._values, key=key)

    def min(self, key=None) -> NT:
        return min(self._values, key=key)

    def __repr__(self):
        return super().__repr__().replace('Stream', 'NumStream')


class Order(abc.ABC):
    @staticmethod
    def natural(x: 'SupportsLessThan') -> 'SupportsLessThan':
        return x

    @staticmethod
    def reverse(x: 'SupportsLessThan') -> 'SupportsLessThan':
        return -x
