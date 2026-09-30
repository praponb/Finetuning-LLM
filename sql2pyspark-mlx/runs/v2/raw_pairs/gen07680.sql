SELECT status FROM orders WHERE shipping_fee < 35 EXCEPT SELECT status FROM payments
