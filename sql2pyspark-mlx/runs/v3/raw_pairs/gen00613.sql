WITH valid_store_sales AS (
    SELECT *
    FROM store_sales
    WHERE units = 16 AND revenue > 3100
),
StoreSalesStats AS (
    SELECT product_id, SUM(revenue) AS total_revenue
    FROM valid_store_sales
    GROUP BY product_id
)
SELECT product_id, total_revenue
FROM StoreSalesStats
WHERE total_revenue < 23000
ORDER BY total_revenue DESC;
