SELECT
    id,
    credit_limit,
    city,
    CAST(credit_limit AS INT) AS credit_limit_int
FROM customers
WHERE credit_limit IS NULL AND email LIKE '%@outlook.com';
