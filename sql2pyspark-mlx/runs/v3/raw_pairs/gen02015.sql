WITH ranked AS (
    SELECT
        id,
        segment,
        credit_limit,
        name,
        ROW_NUMBER() OVER (PARTITION BY segment ORDER BY credit_limit DESC, id) AS pos
    FROM customers
)
SELECT *
FROM ranked
WHERE pos <= 2
ORDER BY credit_limit;
