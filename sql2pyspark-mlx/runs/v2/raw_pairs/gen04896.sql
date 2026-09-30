WITH store_sales_by_store AS (
    SELECT store_id, ROUND(STDDEV(units), 2) AS stddev_units, COUNT(revenue) AS revenue_count
    FROM store_sales
    GROUP BY store_id
)
SELECT
    s.store_name,
    s.opened_date,
    ssbs.stddev_units,
    ssbs.revenue_count
FROM stores s
LEFT OUTER JOIN store_sales_by_store ssbs ON s.id = ssbs.store_id
