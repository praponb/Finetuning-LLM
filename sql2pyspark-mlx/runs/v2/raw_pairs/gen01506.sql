SELECT
    p.brand,
    p.price,
    p.stock_qty,
    i.quantity
FROM products p
INNER JOIN inventory i ON i.product_id = p.id
WHERE p.price <= 930;
