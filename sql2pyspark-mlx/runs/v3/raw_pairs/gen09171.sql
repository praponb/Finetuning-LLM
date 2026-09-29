SELECT i.product_id, i.quantity, ss.product_id AS store_sale_product_id
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.units BETWEEN 16 AND 21
ORDER BY product_id;
