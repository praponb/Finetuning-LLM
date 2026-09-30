WITH StoreStats AS (
    SELECT store_id, MIN(units) AS min_units, ROUND(STDDEV(units), 2) AS stddev_units
    FROM store_sales
    WHERE sale_date > '2025-05-01'
    GROUP BY store_id
)
SELECT
    s.id,
    s.city,
    S.min_units,
    S.stddev_units
FROM stores s
LEFT JOIN StoreStats S ON s.id = S.store_id
ORDER BY s.city;
