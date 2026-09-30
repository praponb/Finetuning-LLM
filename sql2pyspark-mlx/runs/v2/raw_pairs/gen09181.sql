SELECT id, segment, credit_limit
FROM (
    SELECT
        id,
        segment,
        credit_limit,
        ROW_NUMBER() OVER (PARTITION BY segment ORDER BY credit_limit, id) AS pos
    FROM customers
    WHERE segment IS NULL
) AS t
WHERE pos <= 3
ORDER BY credit_limit DESC
