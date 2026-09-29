WITH store_sales_by_store AS (
    SELECT store_id, MAX(revenue) - MIN(revenue) AS revenue_range
    FROM store_sales
    GROUP BY store_id
)
SELECT s.id, s.region, ssbs.revenue_range
FROM stores s
LEFT OUTER JOIN store_sales_by_store ssbs ON s.id = ssbs.store_id
ORDER BY id, s.region;
