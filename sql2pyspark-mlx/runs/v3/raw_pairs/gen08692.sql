SELECT
    i.id,
    i.quantity,
    i.product_id,
    p.cost,
    s.rating
FROM inventory i
LEFT JOIN products p ON i.product_id = p.id
LEFT JOIN suppliers s ON p.supplier_id = s.id
WHERE p.stock_qty <> 106 AND s.rating > 4.2;
