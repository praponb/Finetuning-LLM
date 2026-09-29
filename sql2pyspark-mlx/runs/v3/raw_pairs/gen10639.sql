SELECT
    ss.id,
    ss.revenue,
    ss.units,
    ss.sale_date
FROM store_sales ss
WHERE ss.product_id IN (SELECT id FROM products WHERE stock_qty > 379)
ORDER BY ss.revenue DESC, ss.id DESC
LIMIT 50;
