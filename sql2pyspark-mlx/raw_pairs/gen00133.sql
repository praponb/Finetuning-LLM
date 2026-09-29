SELECT
    s.id,
    s.category,
    s.brand,
    s.cost,
    s.stock_qty,
    s.tags,
    x.last_restock,
    x.inventory_quantity
FROM products s
JOIN LATERAL (
        SELECT c.last_restock, c.quantity AS inventory_quantity
        FROM inventory c
        WHERE c.product_id = s.id
        ORDER BY c.quantity DESC, c.id DESC
        LIMIT 1
    ) x ON TRUE;
