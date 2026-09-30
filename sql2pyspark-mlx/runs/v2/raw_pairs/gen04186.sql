SELECT ss.id, ss.units
FROM store_sales ss
WHERE ss.product_id IN (SELECT id FROM products WHERE ARRAY_CONTAINS(tags, 'clearance'));
