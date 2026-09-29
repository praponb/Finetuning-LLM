WITH store_summary AS (
    SELECT store_id, SUM(units) AS total_units, ROUND(AVG(revenue), 2) AS avg_revenue
    FROM store_sales
    GROUP BY store_id
)
SELECT
    s.id,
    s.sqft,
    ss.total_units,
    ss.avg_revenue
FROM stores s
INNER JOIN store_summary ss ON s.id = ss.store_id
WHERE ss.avg_revenue > 54
ORDER BY ss.total_units ASC, sqft DESC, id ASC
LIMIT 10
