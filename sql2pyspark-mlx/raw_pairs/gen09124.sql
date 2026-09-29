SELECT id, shipping_fee FROM orders WHERE customer_id IS NULL ORDER BY id DESC;
