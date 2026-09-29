SELECT
    i.store_id,
    i.product_id,
    i.last_restock,
    ss.units,
    ss.sale_date,
    ss.store_id AS store_sale_store_id
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.store_id IN (5, 6)
ORDER BY i.product_id DESC
