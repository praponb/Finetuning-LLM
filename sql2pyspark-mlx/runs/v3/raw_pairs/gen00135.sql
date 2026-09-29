SELECT
    id,
    country,
    credit_limit,
    is_active
FROM (
    SELECT
        id,
        country,
        credit_limit,
        is_active,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY credit_limit DESC, id) AS rn
    FROM customers
) AS r
WHERE rn = 1
ORDER BY country;
