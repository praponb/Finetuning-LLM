SELECT i.id, i.product_id, i.store_id
FROM inventory i
WHERE i.store_id IN (SELECT id FROM stores WHERE sqft BETWEEN 10219 AND 13649);
