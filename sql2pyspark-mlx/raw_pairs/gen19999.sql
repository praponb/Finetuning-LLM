WITH store_summary AS (
    SELECT store_id, MIN(revenue) min_revenue
    FROM store_sales
    GROUP BY store_id
)
SELECT s.city, ss.min_revenue
FROM stores s
INNER JOIN store_summary ss ON s.id = ss.store_id
WHERE ss.min_revenue > 1300;
