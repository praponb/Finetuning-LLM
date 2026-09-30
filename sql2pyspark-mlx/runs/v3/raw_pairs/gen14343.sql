SELECT
    i.id,
    i.product_id,
    i.quantity,
    i.store_id,
    ss.units,
    ss.sale_date,
    ss.store_id AS store_sale_store_id
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.store_id IN (1, 6) OR i.last_restock BETWEEN '2023-10-19' AND '2023-11-01';
