SELECT
    i.id,
    i.product_id,
    i.store_id,
    i.last_restock,
    ss.product_id AS store_sale_product_id,
    ss.store_id AS store_sale_store_id,
    ss.sale_date,
    ROUND(i.quantity, 2) AS quantity_rounded
FROM inventory i
LEFT OUTER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.product_id IN (6, 7, 9, 10)
