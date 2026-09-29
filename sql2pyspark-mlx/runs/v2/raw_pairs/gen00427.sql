WITH numbered AS (
    SELECT
        id,
        city,
        credit_limit,
        country,
        DENSE_RANK() OVER (PARTITION BY city ORDER BY credit_limit) AS pos
    FROM customers
)
SELECT
    id,
    city,
    credit_limit,
    country
FROM numbered
WHERE pos <= 2
ORDER BY id DESC, country;
