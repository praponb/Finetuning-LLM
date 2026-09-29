SELECT
    i.product_id,
    i.quantity,
    i.last_restock,
    s.opened_date
FROM inventory i
INNER JOIN stores s ON s.id = i.store_id
WHERE i.quantity = 123 AND i.quantity < 181
ORDER BY product_id DESC
