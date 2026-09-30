SELECT *
FROM customers
WHERE LENGTH(email) >= 7 AND name LIKE 'Ca%'
ORDER BY id DESC;
