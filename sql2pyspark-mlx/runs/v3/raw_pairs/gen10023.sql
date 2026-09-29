SELECT
    ss.id,
    ss.sale_date,
    ss.revenue,
    p.price,
    s.city,
    s.sqft
FROM store_sales ss
LEFT JOIN products p ON p.id = ss.product_id
INNER JOIN stores s ON ss.store_id = s.id
WHERE NOT (ss.product_id IN (2, 7, 9) OR ARRAY_CONTAINS(p.tags, 'clearance'))
ORDER BY sale_date ASC;
