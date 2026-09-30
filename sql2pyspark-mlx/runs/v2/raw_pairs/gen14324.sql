SELECT
    p.supplier_id,
    p.category,
    MIN(i.last_restock) AS min_last_restock,
    MAX(i.last_restock) AS last_last_restock
FROM inventory i
LEFT JOIN products p ON i.product_id = p.id
INNER JOIN stores s ON i.store_id = s.id
GROUP BY p.supplier_id, p.category
ORDER BY min_last_restock;
