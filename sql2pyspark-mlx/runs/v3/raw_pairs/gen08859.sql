SELECT id, age, ROW_NUMBER() OVER (ORDER BY age DESC, id) AS rn
FROM customers
WHERE LENGTH(name) > 11;
