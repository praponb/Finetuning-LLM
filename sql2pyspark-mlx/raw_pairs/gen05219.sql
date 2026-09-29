SELECT
    id,
    country,
    segment,
    signup_date,
    is_active,
    credit_limit,
    age
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY credit_limit NULLS LAST, id) AS rn
    FROM customers
) ranked
WHERE rn = 1
