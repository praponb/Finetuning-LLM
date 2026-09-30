WITH kept_keys AS (
    SELECT region, city
    FROM stores
    EXCEPT
    SELECT region, city
    FROM stores
    WHERE YEAR(opened_date) = 2025
),
kept_rows AS (
    SELECT s.*
    FROM stores s
    INNER JOIN kept_keys k ON s.region = k.region AND s.city = k.city
)
SELECT
    city,
    region,
    sqft
FROM kept_rows
ORDER BY region NULLS FIRST, sqft
