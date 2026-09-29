SELECT
    p.brand,
    p.category,
    COUNT(*) AS cnt,
    MAX(s.unit_price) AS max_unit_price,
    MAX(s.quantity) AS max_quantity
FROM order_items s
INNER JOIN products p ON s.product_id = p.id
GROUP BY p.brand, p.category
ORDER BY category NULLS LAST;
