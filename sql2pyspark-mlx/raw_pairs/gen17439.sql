SELECT id, sale_date, store_id
FROM store_sales
WHERE units IS NOT NULL
UNION
SELECT id, sale_date, store_id
FROM store_sales
WHERE product_id IN (3, 4, 9);
