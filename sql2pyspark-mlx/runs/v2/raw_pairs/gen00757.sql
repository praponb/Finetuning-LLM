SELECT
    i.id,
    i.last_restock,
    i.product_id,
    s.opened_date
FROM inventory i
INNER JOIN stores s ON s.id = i.store_id
WHERE s.region IS NULL AND i.quantity <= 88
ORDER BY product_id DESC, opened_date DESC
