SELECT *
FROM (
    SELECT
        id,
        city,
        credit_limit,
        DENSE_RANK() OVER (PARTITION BY city ORDER BY credit_limit DESC) AS pos
    FROM customers
) AS t
WHERE pos <= 1;
