SELECT
    i.id,
    i.last_restock,
    p.price,
    p.stock_qty,
    s.store_name
FROM inventory i
LEFT JOIN products p ON i.product_id = p.id
LEFT JOIN stores s ON i.store_id = s.id
WHERE s.store_name IS NOT NULL;
