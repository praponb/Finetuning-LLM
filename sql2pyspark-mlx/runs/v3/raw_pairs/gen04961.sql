SELECT *
FROM customers
WHERE credit_limit >= 10100 OR country IN ('TH', 'UK')
