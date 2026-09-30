SELECT
    p.id,
    p.stock_qty,
    p.category,
    t.quantity_count,
    t.max_quantity
FROM products p
INNER JOIN (SELECT product_id, COUNT(quantity) quantity_count, MAX(quantity) max_quantity
    FROM inventory
    WHERE quantity BETWEEN 120 AND 212
    GROUP BY product_id) t ON p.id = t.product_id
ORDER BY quantity_count DESC;
