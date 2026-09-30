SELECT
    p.cost,
    p.price,
    i.last_restock,
    i.product_id
FROM products p
LEFT JOIN inventory i ON p.id = i.product_id
ORDER BY p.cost
