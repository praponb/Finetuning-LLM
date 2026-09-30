SELECT
    i.quantity,
    p.product_name,
    p.price,
    s.supplier_name,
    s.city
FROM inventory i
INNER JOIN products p ON i.product_id = p.id
INNER JOIN suppliers s ON p.supplier_id = s.id
WHERE s.country != 'JP' AND YEAR(i.last_restock) >= 2024
ORDER BY i.quantity, 4 DESC
