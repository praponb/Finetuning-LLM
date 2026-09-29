WITH StoreSummary AS (
    SELECT store_id, COUNT(revenue) AS revenue_count, ROUND(AVG(revenue), 2) AS avg_revenue
    FROM store_sales
    GROUP BY store_id
)
SELECT stores.city, StoreSummary.revenue_count, StoreSummary.avg_revenue
FROM stores
INNER JOIN StoreSummary ON stores.id = StoreSummary.store_id
ORDER BY revenue_count
