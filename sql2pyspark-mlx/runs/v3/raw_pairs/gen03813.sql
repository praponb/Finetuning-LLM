SELECT
    i.id,
    i.product_id,
    i.last_restock,
    i.quantity,
    ss.sale_date,
    ss.store_id
FROM inventory i
INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
WHERE ss.product_id IN (3, 4, 8) OR ss.sale_date < '2023-03-01'
ORDER BY i.quantity, id DESC;
