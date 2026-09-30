SELECT id, country, credit_limit
FROM (
    SELECT
        id,
        country,
        credit_limit,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY credit_limit, id) AS pos
    FROM customers
) AS r
WHERE pos <= 3
