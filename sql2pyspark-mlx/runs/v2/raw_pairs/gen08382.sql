SELECT
    p.category,
    MONTH(p.launch_date) AS launch_month,
    MIN(i.quantity) AS min_quantity,
    COUNT(i.quantity) AS quantity_count
FROM inventory i
LEFT JOIN stores s ON i.store_id = s.id
INNER JOIN products p ON i.product_id = p.id
WHERE p.supplier_id = 23
GROUP BY p.category, MONTH(p.launch_date)
ORDER BY min_quantity, 4 DESC
