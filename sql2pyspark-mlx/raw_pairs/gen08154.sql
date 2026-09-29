SELECT id, product_id
FROM store_sales
WHERE store_id IN (3, 4, 5, 6)
ORDER BY id DESC
LIMIT 10;
