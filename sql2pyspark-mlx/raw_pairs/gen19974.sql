WITH store_summary AS (
    SELECT store_id, COUNT(units) AS units_count, SUM(units) AS total_units
    FROM store_sales
    WHERE revenue >= 1500
    GROUP BY store_id
)
SELECT
    s.city,
    s.sqft,
    ss.units_count,
    ss.total_units
FROM stores s
LEFT JOIN store_summary ss ON s.id = ss.store_id
