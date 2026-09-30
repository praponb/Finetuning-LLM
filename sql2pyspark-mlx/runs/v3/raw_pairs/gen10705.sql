SELECT
    i.product_id,
    i.quantity,
    i.store_id,
    ss.store_id AS store_sale_store_id
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.sale_date < '2024-06-01' AND ss.units = 25
