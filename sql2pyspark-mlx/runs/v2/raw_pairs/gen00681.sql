WITH store_summary AS (
    SELECT store_id, MAX(revenue) - MIN(revenue) AS revenue_range
    FROM store_sales
    GROUP BY store_id
)
SELECT
    s.id,
    s.city,
    s.store_name,
    ss.revenue_range
FROM stores s
INNER JOIN store_summary ss ON s.id = ss.store_id
