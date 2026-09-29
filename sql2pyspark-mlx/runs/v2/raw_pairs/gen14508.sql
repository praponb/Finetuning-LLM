SELECT product_id
FROM store_sales
WHERE MONTH(sale_date) IN (4, 6, 11)
UNION
SELECT product_id
FROM store_sales
WHERE product_id IN (4, 9)
