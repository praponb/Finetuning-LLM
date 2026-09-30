SELECT ss.id, ss.store_id, ss.sale_date
FROM store_sales ss
WHERE ss.store_id IN (SELECT id FROM stores WHERE LOWER(store_name) LIKE '%iam%')
ORDER BY ss.store_id DESC
