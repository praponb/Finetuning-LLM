SELECT id, country
FROM suppliers
WHERE country IN ('UK', 'DE')
UNION
SELECT id, country
FROM suppliers
WHERE city IN ('Berlin', 'Bangkok', 'Tokyo');
