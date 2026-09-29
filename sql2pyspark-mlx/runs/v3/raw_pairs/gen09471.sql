SELECT
    ss.id,
    ss.revenue,
    ss.sale_date,
    p.supplier_id,
    p.brand,
    s.city,
    s.supplier_name,
    LENGTH(s.country) AS country_length
FROM store_sales ss
JOIN products p ON p.id = ss.product_id
LEFT OUTER JOIN suppliers s ON p.supplier_id = s.id
WHERE p.category IN ('Sports', 'Electronics')
ORDER BY id DESC
