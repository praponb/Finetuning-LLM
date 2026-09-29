SELECT orders.id, orders.customer_id, orders.channel
FROM orders
WHERE orders.store_id NOT IN (SELECT id FROM stores WHERE city <> 'Tokyo')
