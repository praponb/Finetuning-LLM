SELECT id, shipping_fee, amount FROM orders WHERE shipping_fee >= (SELECT MAX(shipping_fee) FROM orders)
