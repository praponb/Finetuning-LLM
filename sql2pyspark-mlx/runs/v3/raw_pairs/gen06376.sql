SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC, id) AS rnk FROM suppliers WHERE city != 'Singapore') AS r WHERE rnk = 1;
