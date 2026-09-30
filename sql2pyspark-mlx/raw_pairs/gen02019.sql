SELECT
    ss.id,
    ss.sale_date,
    ss.product_id,
    ss.revenue
FROM store_sales ss
WHERE ss.store_id NOT IN (SELECT id FROM stores WHERE store_name IS NOT NULL)
