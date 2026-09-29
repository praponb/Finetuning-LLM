SELECT revenue, store_id, product_id
FROM store_sales
WHERE store_id IN (1, 3)
ORDER BY store_id ASC;
