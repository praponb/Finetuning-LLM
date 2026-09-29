SELECT
    i.id,
    i.product_id,
    i.last_restock,
    p.product_name,
    p.price,
    p.stock_qty
FROM inventory i
JOIN products p ON p.id = i.product_id;
