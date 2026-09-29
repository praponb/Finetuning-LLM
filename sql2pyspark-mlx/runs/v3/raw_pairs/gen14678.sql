SELECT amount
FROM payments
EXCEPT
SELECT shipping_fee
FROM orders
