SELECT id, shipping_fee, order_date, customer_id FROM orders WHERE shipping_fee > (SELECT MAX(shipping_fee) FROM orders) ORDER BY order_date DESC, shipping_fee DESC;
