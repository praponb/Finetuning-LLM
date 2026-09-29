WITH store_sales_by_store AS (
    SELECT store_id, ROUND(STDDEV(revenue), 2) AS stddev_revenue
    FROM store_sales
    GROUP BY store_id
)
SELECT s.region, ssbs.stddev_revenue
FROM stores s
LEFT JOIN store_sales_by_store ssbs ON s.id = ssbs.store_id
ORDER BY region ASC, ssbs.stddev_revenue ASC
