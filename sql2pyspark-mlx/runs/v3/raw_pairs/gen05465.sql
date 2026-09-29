SELECT
    p.category,
    CASE WHEN p.brand IN ('Acme', 'Initech') THEN 'tier_1' ELSE 'other' END AS brand_group,
    ROUND(AVG(ss.revenue), 2) AS avg_revenue,
    MIN(ss.sale_date) AS first_sale_date
FROM store_sales ss
LEFT OUTER JOIN products p ON p.id = ss.product_id
JOIN suppliers s ON p.supplier_id = s.id
WHERE ss.revenue < 1300
GROUP BY p.category, CASE WHEN p.brand IN ('Acme', 'Initech') THEN 'tier_1' ELSE 'other' END
ORDER BY 2
