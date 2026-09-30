SELECT stores.id, stores.city
FROM stores
WHERE stores.store_name NOT LIKE 'D%' AND NOT EXISTS (SELECT 1 FROM orders WHERE orders.store_id = stores.id AND orders.customer_id = 23)
ORDER BY stores.id ASC;
