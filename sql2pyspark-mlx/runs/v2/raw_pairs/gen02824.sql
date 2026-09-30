SELECT
    i.quantity,
    p.cost,
    p.stock_qty,
    s.store_name
FROM inventory i
RIGHT JOIN products p ON i.product_id = p.id
JOIN stores s ON s.id = i.store_id;
