SELECT *
FROM customers
WHERE age > 58 AND segment IN ('Corporate', 'SMB', 'Consumer');
