SELECT
    p.stock_qty,
    p.price,
    s.city,
    LOWER(s.city) AS city_lower
FROM products p
JOIN suppliers s ON p.supplier_id = s.id
ORDER BY stock_qty DESC, city_lower
