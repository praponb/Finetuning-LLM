SELECT
    id,
    is_active,
    credit_limit,
    email,
    TRIM(segment) AS clean_segment
FROM customers
WHERE signup_date BETWEEN '2023-09-04' AND '2025-01-01'
