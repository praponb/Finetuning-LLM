SELECT id, store_id
FROM store_sales
WHERE revenue <= 3600 AND product_id IN (6, 8, 9) AND units IS NOT NULL
