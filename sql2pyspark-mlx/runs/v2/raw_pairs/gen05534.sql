SELECT country FROM suppliers WHERE city IN ('Berlin', 'London', 'Tokyo') INTERSECT SELECT country FROM sessions;
