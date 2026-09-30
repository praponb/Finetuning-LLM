SELECT
    id,
    email,
    country,
    city,
    is_active,
    credit_limit,
    age
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY credit_limit DESC, id) AS dedup_rank
    FROM customers
) t
WHERE dedup_rank = 1
