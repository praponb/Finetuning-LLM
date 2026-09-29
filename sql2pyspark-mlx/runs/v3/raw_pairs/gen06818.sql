SELECT
    ss.id,
    ss.product_id,
    ss.units,
    ss.revenue
FROM store_sales ss
WHERE ss.product_id = 7 AND ss.product_id NOT IN (SELECT id FROM products WHERE cost < 560);
