SELECT ss.id, ss.sale_date, ss.product_id
FROM store_sales ss
WHERE ss.revenue IS NOT NULL AND ss.store_id IN (SELECT id FROM stores WHERE sqft >= 5387)
ORDER BY ss.product_id DESC;
