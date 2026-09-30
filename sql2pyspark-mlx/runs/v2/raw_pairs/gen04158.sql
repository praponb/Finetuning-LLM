SELECT
    p.supplier_id,
    p.price,
    p.cost,
    i.last_restock,
    i.product_id
FROM products p
LEFT JOIN inventory i ON i.product_id = p.id
WHERE i.product_id IN (1, 9)
ORDER BY product_id DESC;
