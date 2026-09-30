WITH store_sales_by_store AS (
    SELECT store_id, COUNT(DISTINCT revenue) AS unique_products
    FROM store_sales
    WHERE product_id IN (4, 5)
    GROUP BY store_id
)
SELECT s.id, s.region, ssbs.unique_products
FROM stores s
LEFT OUTER JOIN store_sales_by_store ssbs ON s.id = ssbs.store_id
WHERE ssbs.unique_products >= 6
ORDER BY region ASC;
