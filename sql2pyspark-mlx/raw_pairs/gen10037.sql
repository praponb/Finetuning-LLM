SELECT
    p.price,
    p.stock_qty,
    p.product_name,
    i.product_id
FROM products p
INNER JOIN inventory i ON p.id = i.product_id
WHERE i.store_id IN (1, 5)
