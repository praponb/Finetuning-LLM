SELECT
    p.category,
    p.brand,
    AVG(i.quantity) AS avg_quantity,
    MIN(i.quantity) AS min_quantity
FROM inventory i
INNER JOIN products p ON i.product_id = p.id
LEFT JOIN suppliers s ON p.supplier_id = s.id
GROUP BY p.category, p.brand
