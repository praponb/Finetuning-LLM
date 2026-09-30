WITH store_sales_by_store AS (
    SELECT store_id, MAX(sale_date) AS latest_sale_date, AVG(units) AS avg_units
    FROM store_sales
    GROUP BY store_id
)
SELECT
    s.id,
    s.region,
    ssbs.latest_sale_date,
    ssbs.avg_units
FROM stores s
LEFT JOIN store_sales_by_store ssbs ON s.id = ssbs.store_id
ORDER BY ssbs.avg_units;
