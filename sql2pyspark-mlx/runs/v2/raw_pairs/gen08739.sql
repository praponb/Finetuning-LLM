SELECT
    ss.revenue,
    ss.sale_date,
    p.brand,
    p.product_name,
    s.country,
    ROUND(p.cost * 1.15, 2) AS cost_adjusted
FROM store_sales ss
JOIN products p ON ss.product_id = p.id
LEFT OUTER JOIN suppliers s ON p.supplier_id = s.id
