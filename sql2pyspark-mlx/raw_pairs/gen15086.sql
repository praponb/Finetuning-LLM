SELECT
    p.brand,
    p.category,
    COUNT(i.quantity) AS quantity_count,
    ROUND(STDDEV(i.quantity), 2) AS stddev_quantity
FROM inventory i
INNER JOIN products p ON i.product_id = p.id
LEFT JOIN suppliers s ON p.supplier_id = s.id
GROUP BY p.brand, p.category
ORDER BY stddev_quantity DESC, category;
