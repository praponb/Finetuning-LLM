SELECT
    d.category,
    COUNT(*) AS num_rows,
    SUM(s.unit_price) AS total_unit_price
FROM order_items s
LEFT JOIN products d ON s.product_id = d.id
GROUP BY d.category
ORDER BY category;
