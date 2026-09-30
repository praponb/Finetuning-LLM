WITH store_sales_by_store AS (
    SELECT store_id, SUM(revenue) AS total_revenue
    FROM store_sales
    GROUP BY store_id
)
SELECT s.id, s.opened_date, ssbs.total_revenue
FROM stores s
INNER JOIN store_sales_by_store ssbs ON s.id = ssbs.store_id
ORDER BY total_revenue DESC;
