SELECT
    oi.id,
    oi.order_id,
    p.price,
    s.supplier_name,
    s.rating,
    LOWER(s.city) AS city_lower
FROM order_items oi
FULL OUTER JOIN products p ON oi.product_id = p.id
LEFT JOIN suppliers s ON p.supplier_id = s.id
ORDER BY rating DESC
