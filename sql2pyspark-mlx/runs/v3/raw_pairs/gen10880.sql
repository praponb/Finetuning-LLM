SELECT
    credit_limit,
    segment,
    city,
    CAST(is_active AS INT) is_active_flag
FROM customers
WHERE country = 'US' AND age = 34;
