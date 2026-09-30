SELECT name
FROM customers
WHERE credit_limit < 9300
UNION
SELECT name
FROM customers
WHERE segment IN ('Corporate', 'SMB');
