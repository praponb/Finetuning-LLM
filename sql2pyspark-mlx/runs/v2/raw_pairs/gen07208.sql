SELECT id, shipping_fee, discount, customer_id FROM orders WHERE shipping_fee >= (SELECT AVG(shipping_fee) FROM orders) ORDER BY discount, customer_id DESC;
