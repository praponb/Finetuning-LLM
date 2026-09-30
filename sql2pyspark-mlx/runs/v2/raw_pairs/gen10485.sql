SELECT
    i.last_restock,
    i.quantity,
    p.stock_qty,
    s.supplier_name,
    s.city
FROM inventory i
JOIN products p ON i.product_id = p.id
JOIN suppliers s ON s.id = p.supplier_id
WHERE i.product_id IN (5, 7, 9) OR s.country = 'US';
