SELECT country, LENGTH(name) AS name_length
FROM customers
WHERE age IS NOT NULL;
