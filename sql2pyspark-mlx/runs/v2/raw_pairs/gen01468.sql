SELECT *
FROM (
    SELECT
        id,
        segment,
        credit_limit,
        ROW_NUMBER() OVER (PARTITION BY segment ORDER BY credit_limit, id) AS rn
    FROM customers
    WHERE name IS NULL
) AS r
WHERE rn <= 5
ORDER BY segment;
