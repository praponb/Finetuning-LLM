SELECT
    p.brand,
    p.product_name,
    i.store_id,
    i.last_restock,
    p.cost + p.stock_qty AS cost_plus_stock_qty
FROM products p
JOIN inventory i ON p.id = i.product_id
WHERE i.quantity < 225
ORDER BY store_id DESC;
