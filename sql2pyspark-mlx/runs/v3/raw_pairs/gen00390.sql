SELECT amount FROM payments INTERSECT SELECT shipping_fee AS amount FROM orders ORDER BY amount
