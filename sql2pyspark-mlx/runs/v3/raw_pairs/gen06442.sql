SELECT
    ss.id,
    ss.product_id,
    ss.sale_date,
    ss.revenue
FROM store_sales ss
WHERE ss.units <= 27 AND ss.product_id NOT IN (SELECT id FROM products WHERE LENGTH(product_name) < 14)
ORDER BY 2 DESC, sale_date DESC;
