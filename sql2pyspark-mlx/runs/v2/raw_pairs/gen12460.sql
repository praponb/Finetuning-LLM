WITH ranked AS (
    SELECT
        id,
        segment,
        credit_limit,
        ROW_NUMBER() OVER (PARTITION BY segment ORDER BY credit_limit DESC, id) AS pos
    FROM customers
)
SELECT id, segment, credit_limit
FROM ranked
WHERE pos <= 3;
