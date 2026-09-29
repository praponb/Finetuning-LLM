SELECT
    i.id,
    i.store_id,
    i.last_restock,
    ss.store_id AS store_sale_store_id,
    ss.product_id,
    ss.sale_date
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.product_id IN (1, 4, 6, 9) OR YEAR(i.last_restock) = 2024
