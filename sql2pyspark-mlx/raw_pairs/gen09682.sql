SELECT 'order' AS origin, shipping_fee FROM orders UNION SELECT 'payment' AS origin, amount FROM payments ORDER BY shipping_fee;
