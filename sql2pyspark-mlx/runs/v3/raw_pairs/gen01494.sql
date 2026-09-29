SELECT stores.id, stores.opened_date
FROM stores
WHERE stores.city = 'London' AND NOT EXISTS (SELECT 1 FROM orders WHERE orders.store_id = stores.id AND orders.store_id IS NULL)
ORDER BY stores.id DESC;
