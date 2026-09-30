SELECT stores.id, stores.opened_date, stores.sqft
FROM stores
WHERE NOT EXISTS (SELECT 1 FROM orders WHERE orders.store_id = stores.id AND orders.status <> 'SHIPPED')
ORDER BY stores.sqft DESC, id ASC;
