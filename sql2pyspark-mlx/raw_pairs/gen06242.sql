SELECT
    ss.id,
    ss.units,
    ss.product_id,
    ss.sale_date
FROM store_sales ss
WHERE ss.store_id IN (SELECT id FROM stores WHERE city = 'London')
ORDER BY ss.product_id DESC;
