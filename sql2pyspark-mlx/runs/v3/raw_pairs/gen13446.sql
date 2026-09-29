SELECT
    s.id,
    s.city,
    COUNT(i.id) AS inventory_count,
    COUNT(DISTINCT i.quantity) AS unique_products,
    AVG(i.quantity) AS avg_quantity
FROM stores s
LEFT OUTER JOIN inventory i ON s.id = i.store_id
WHERE i.product_id = 5
GROUP BY s.id, s.city
ORDER BY city;
