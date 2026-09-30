SELECT
    ss.id,
    ss.revenue,
    p.category,
    s.rating,
    s.country
FROM store_sales ss
JOIN products p ON ss.product_id = p.id
LEFT OUTER JOIN suppliers s ON p.supplier_id = s.id
WHERE NOT (s.country = 'JP' AND p.stock_qty >= 262);
