WITH store_sales_by_store AS (
    SELECT store_id, MAX(sale_date) AS last_sale_date
    FROM store_sales
    GROUP BY store_id
)
SELECT s.store_name, ssbs.last_sale_date
FROM stores s
INNER JOIN store_sales_by_store ssbs ON s.id = ssbs.store_id;
