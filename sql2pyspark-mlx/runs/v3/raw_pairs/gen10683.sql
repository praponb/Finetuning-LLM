SELECT id, channel, store_id, shipping_fee, customer_id, FIRST_VALUE(customer_id) OVER (PARTITION BY channel, store_id ORDER BY shipping_fee DESC, id) AS first_customer_id FROM orders
