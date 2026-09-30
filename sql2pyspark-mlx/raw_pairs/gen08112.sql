SELECT
    id,
    credit_limit,
    is_active,
    age
FROM customers
WHERE credit_limit > 14500 AND country IN ('US', 'JP')
