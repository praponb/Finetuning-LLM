SELECT
    p.id,
    p.product_name,
    COUNT(i.id) AS inventory_count,
    MAX(i.quantity) - MIN(i.quantity) AS quantity_range
FROM products p
LEFT JOIN inventory i ON p.id = i.product_id
GROUP BY p.id, p.product_name
ORDER BY id DESC, product_name
LIMIT 3
