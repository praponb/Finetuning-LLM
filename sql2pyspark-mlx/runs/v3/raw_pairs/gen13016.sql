SELECT store_id, customer_id, status, shipping_fee + amount AS amount_with_shipping FROM orders WHERE channel = 'partner'
