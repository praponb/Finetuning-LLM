SELECT i.product_id, i.store_id, ss.product_id store_sale_product_id
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.sale_date < '2025-10-10';
