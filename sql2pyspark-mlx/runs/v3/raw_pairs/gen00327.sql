SELECT ss.id, ss.product_id
FROM store_sales ss
WHERE ss.store_id NOT IN (SELECT id FROM stores WHERE opened_date >= '2025-01-01')
ORDER BY product_id DESC, id DESC
LIMIT 3;
