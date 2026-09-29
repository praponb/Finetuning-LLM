SELECT country, 'session' AS origin
FROM sessions
UNION
SELECT city, 'supplier' AS origin
FROM suppliers;
