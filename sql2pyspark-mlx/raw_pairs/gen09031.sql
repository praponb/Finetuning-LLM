WITH store_summary AS (
    SELECT store_id, COUNT(*) AS total_count, ROUND(STDDEV(revenue), 2) AS stddev_revenue
    FROM store_sales
    GROUP BY store_id
)
SELECT
    s.id,
    s.region,
    s.store_name,
    ss.total_count,
    ss.stddev_revenue
FROM stores s
INNER JOIN store_summary ss ON s.id = ss.store_id
