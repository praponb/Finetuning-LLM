SELECT
    p.product_name,
    ss.revenue,
    ss.sale_date,
    ss.units
FROM products p
LEFT JOIN store_sales ss ON p.id = ss.product_id
WHERE ss.units <= 30
ORDER BY units, product_name;
