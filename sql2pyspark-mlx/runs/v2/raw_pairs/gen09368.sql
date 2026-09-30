SELECT
    ss.revenue,
    ss.store_id,
    p.price,
    s.country,
    ROUND(s.rating, 1) AS rating_rounded
FROM store_sales ss
JOIN products p ON p.id = ss.product_id
LEFT JOIN suppliers s ON s.id = p.supplier_id
WHERE s.country IN ('US', 'JP')
