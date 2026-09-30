SELECT
    i.id,
    i.store_id,
    i.quantity,
    i.last_restock,
    p.brand,
    p.stock_qty
FROM inventory i
JOIN products p ON p.id = i.product_id
WHERE p.supplier_id IN (6, 12, 24, 31)
ORDER BY i.quantity, id
LIMIT 5;
