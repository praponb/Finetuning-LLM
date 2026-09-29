SELECT *, discount - amount AS discount_minus_amount
FROM orders
WHERE shipping_fee <= 24 AND amount > 440;
