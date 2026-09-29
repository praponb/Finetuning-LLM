SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY age DESC, id) AS rnk FROM customers) AS r WHERE rnk <= 5 ORDER BY age;
