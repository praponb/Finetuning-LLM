SELECT
    p.id,
    p.brand,
    COUNT(i.id) AS inventory_count,
    MAX(i.quantity) AS max_quantity,
    COUNT(DISTINCT i.quantity) AS unique_stores
FROM products p
INNER JOIN inventory i ON i.product_id = p.id
GROUP BY p.id, p.brand
HAVING COUNT(i.id) > 1
ORDER BY unique_stores DESC, brand;
