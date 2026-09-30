SELECT
    p.category,
    p.brand,
    ROUND(AVG(i.quantity), 2) AS avg_quantity,
    ROUND(STDDEV(i.quantity), 2) AS stddev_quantity
FROM inventory i
INNER JOIN products p ON i.product_id = p.id
FULL OUTER JOIN stores s ON s.id = i.store_id
GROUP BY p.category, p.brand
ORDER BY avg_quantity;
