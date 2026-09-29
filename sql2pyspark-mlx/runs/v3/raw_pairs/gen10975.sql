SELECT
    i.id,
    i.quantity,
    i.store_id,
    p.stock_qty,
    s.country
FROM inventory i
JOIN products p ON i.product_id = p.id
LEFT JOIN suppliers s ON p.supplier_id = s.id
WHERE p.price < 420 AND MONTH(p.launch_date) IN (1, 6, 8);
