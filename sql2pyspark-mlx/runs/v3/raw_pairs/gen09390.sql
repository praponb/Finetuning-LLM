SELECT segment
FROM customers
WHERE is_active
UNION
SELECT segment
FROM customers
WHERE country IN ('JP', 'DE')
