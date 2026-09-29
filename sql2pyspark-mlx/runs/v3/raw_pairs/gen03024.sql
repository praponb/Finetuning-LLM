WITH store_sales_by_store AS (
    SELECT store_id, MAX(units) AS max_units
    FROM store_sales
    GROUP BY store_id
)
SELECT s.id, s.store_name, ssbs.max_units
FROM stores s
JOIN store_sales_by_store ssbs ON s.id = ssbs.store_id
