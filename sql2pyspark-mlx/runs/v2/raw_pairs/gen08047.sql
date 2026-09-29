SELECT
    p.product_name,
    p.stock_qty,
    p.category,
    i.last_restock,
    i.quantity
FROM products p
LEFT OUTER JOIN inventory i ON p.id = i.product_id
ORDER BY stock_qty DESC;
