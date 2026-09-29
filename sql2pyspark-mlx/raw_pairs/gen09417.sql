SELECT
    s.city,
    s.country,
    MAX(i.last_restock) AS latest_last_restock,
    COUNT(*) AS row_count
FROM inventory i
LEFT OUTER JOIN products p ON i.product_id = p.id
INNER JOIN suppliers s ON p.supplier_id = s.id
WHERE i.store_id IN (1, 3, 4, 6)
GROUP BY s.city, s.country
HAVING COUNT(*) > 5
ORDER BY row_count DESC, city ASC, country ASC
LIMIT 5;
