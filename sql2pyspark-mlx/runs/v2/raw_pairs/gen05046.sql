SELECT stores.id, stores.city
FROM stores
WHERE stores.region NOT IN ('South', 'Central') AND NOT EXISTS (SELECT 1 FROM orders WHERE orders.store_id = stores.id)
