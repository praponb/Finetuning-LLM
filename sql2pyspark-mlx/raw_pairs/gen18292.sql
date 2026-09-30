SELECT
    id,
    region,
    opened_date,
    sqft
FROM (
    SELECT *, DENSE_RANK() OVER (PARTITION BY region ORDER BY opened_date) AS rn
    FROM stores
) AS r
WHERE rn <= 1
ORDER BY id DESC;
