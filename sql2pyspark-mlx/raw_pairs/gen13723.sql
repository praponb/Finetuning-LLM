SELECT id, name
FROM customers
WHERE credit_limit < 10300
INTERSECT
SELECT id, name
FROM customers
WHERE segment NOT IN ('Corporate', 'Consumer');
