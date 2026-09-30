SELECT stores.id, stores.store_name, stores.opened_date
FROM stores
WHERE stores.id IN (SELECT store_id FROM orders WHERE status NOT IN ('COMPLETED', 'SHIPPED', 'PENDING'))
ORDER BY stores.store_name, stores.id
