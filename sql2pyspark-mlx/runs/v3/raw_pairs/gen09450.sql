SELECT
    id,
    region,
    sqft,
    city
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft, id) AS rn
    FROM stores
    WHERE MONTH(opened_date) IN (8, 9, 10)
) AS t
WHERE rn <= 2
ORDER BY region, city;
