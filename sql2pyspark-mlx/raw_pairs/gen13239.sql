WITH store_sales_by_store AS (
    SELECT store_id, AVG(revenue) AS avg_revenue
    FROM store_sales
    GROUP BY store_id
)
SELECT s.sqft, ssbs.avg_revenue
FROM stores s
LEFT JOIN store_sales_by_store ssbs ON s.id = ssbs.store_id
ORDER BY sqft ASC;
