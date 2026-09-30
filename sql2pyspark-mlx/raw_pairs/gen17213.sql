SELECT id, country, credit_limit
FROM (
    SELECT
        id,
        country,
        credit_limit,
        RANK() OVER (PARTITION BY country ORDER BY credit_limit DESC) AS rn
    FROM customers
) AS ranked
WHERE rn <= 5
ORDER BY credit_limit
