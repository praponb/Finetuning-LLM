SELECT
    ss.id,
    ss.revenue,
    p.category,
    s.rating
FROM store_sales ss
JOIN products p ON ss.product_id = p.id
FULL OUTER JOIN suppliers s ON p.supplier_id = s.id
ORDER BY id DESC
