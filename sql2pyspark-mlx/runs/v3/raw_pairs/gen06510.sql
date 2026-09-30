SELECT
    ss.id,
    ss.revenue,
    ss.product_id,
    ss.store_id
FROM store_sales ss
WHERE ss.store_id NOT IN (SELECT id FROM stores WHERE sqft < 8163);
