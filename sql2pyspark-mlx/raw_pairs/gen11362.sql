SELECT amount FROM payments UNION SELECT shipping_fee AS amount FROM orders WHERE discount BETWEEN 0.17 AND 0.18
