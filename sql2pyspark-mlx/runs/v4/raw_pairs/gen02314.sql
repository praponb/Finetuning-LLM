SELECT id, shipping_fee, discount, customer_id FROM orders WHERE shipping_fee < (SELECT MIN(shipping_fee) FROM orders);
