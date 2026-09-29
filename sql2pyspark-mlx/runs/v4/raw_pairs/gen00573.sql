SELECT
    i.id,
    i.store_id,
    i.product_id,
    p.stock_qty,
    s.city
FROM inventory i
JOIN products p ON i.product_id = p.id
LEFT OUTER JOIN stores s ON s.id = i.store_id
WHERE p.category <> 'Grocery' AND i.quantity > 139
ORDER BY stock_qty DESC, city
