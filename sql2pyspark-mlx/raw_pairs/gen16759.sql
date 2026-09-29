SELECT
    ss.id,
    ss.revenue,
    ss.units,
    ss.sale_date
FROM store_sales ss
WHERE ss.product_id NOT IN (SELECT id FROM products WHERE SIZE(tags) >= 1);
