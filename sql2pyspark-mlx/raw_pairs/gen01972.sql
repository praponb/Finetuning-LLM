WITH store_stats AS (
    SELECT store_id, MAX(revenue) - MIN(revenue) AS revenue_range, MIN(units) AS min_units
    FROM store_sales
    GROUP BY store_id
)
SELECT
    s.id,
    s.opened_date,
    s.region,
    ss.revenue_range,
    ss.min_units
FROM stores s
LEFT JOIN store_stats ss ON s.id = ss.store_id
ORDER BY s.opened_date DESC;
