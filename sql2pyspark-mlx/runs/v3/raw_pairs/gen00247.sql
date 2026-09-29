SELECT
    s.country,
    s.city,
    MIN(i.last_restock) AS first_last_restock,
    ROUND(STDDEV(i.quantity), 2) AS stddev_quantity
FROM inventory i
RIGHT JOIN products p ON i.product_id = p.id
JOIN suppliers s ON p.supplier_id = s.id
WHERE i.product_id IN (5, 6, 7, 10)
GROUP BY s.country, s.city
ORDER BY s.city DESC
