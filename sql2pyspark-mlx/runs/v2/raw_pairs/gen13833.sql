SELECT
    s.city,
    s.country,
    MIN(i.quantity) AS min_quantity,
    MAX(i.last_restock) AS latest_last_restock
FROM inventory i
INNER JOIN products p ON p.id = i.product_id
LEFT JOIN suppliers s ON p.supplier_id = s.id
GROUP BY s.city, s.country;
