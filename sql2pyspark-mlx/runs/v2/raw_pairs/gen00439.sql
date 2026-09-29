SELECT
    p.brand,
    ss.sale_date,
    ss.revenue,
    ss.units
FROM products p
LEFT OUTER JOIN store_sales ss ON p.id = ss.product_id
WHERE ss.sale_date <= '2025-04-01'
