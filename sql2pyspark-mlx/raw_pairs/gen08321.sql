WITH ranked AS (
    SELECT
        id,
        country,
        credit_limit,
        city,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY credit_limit DESC, id) AS rnk
    FROM customers
)
SELECT *
FROM ranked
WHERE rnk <= 2
ORDER BY credit_limit DESC;
