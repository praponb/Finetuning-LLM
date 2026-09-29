WITH store_sales_by_store AS (
    SELECT store_id, COUNT(revenue) AS revenue_count
    FROM store_sales
    GROUP BY store_id
)
SELECT s.id, s.sqft, ssbs.revenue_count
FROM stores s
INNER JOIN store_sales_by_store ssbs ON s.id = ssbs.store_id
WHERE ssbs.revenue_count > 8;
