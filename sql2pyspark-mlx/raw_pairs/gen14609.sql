SELECT status FROM orders WHERE store_id IN (6, 20, 32, 33) EXCEPT SELECT status FROM payments WHERE method NOT IN ('card', 'promptpay');
