SELECT 'supplier' AS origin, country
FROM suppliers
UNION
SELECT 'session' AS origin, country
FROM sessions;
