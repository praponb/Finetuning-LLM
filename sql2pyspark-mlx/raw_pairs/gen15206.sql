SELECT country
FROM suppliers
UNION
SELECT country
FROM sessions
WHERE device != 'mobile';
