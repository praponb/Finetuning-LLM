SELECT o.id, o.shipping_fee
FROM orders o
WHERE o.amount BETWEEN 590 AND 1150 AND o.customer_id NOT IN (SELECT id FROM customers WHERE city NOT IN ('Tokyo', 'New York', 'Singapore'));
