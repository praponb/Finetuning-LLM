SELECT id, product_id, store_id
FROM store_sales
WHERE revenue < 3800
UNION ALL
SELECT id, product_id, store_id
FROM store_sales
WHERE MONTH(sale_date) IN (4, 6, 8)
