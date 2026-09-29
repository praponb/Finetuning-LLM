SELECT o.id, o.discount, o.shipping_fee
FROM orders o
WHERE o.customer_id NOT IN (SELECT id FROM customers WHERE age != 32)
ORDER BY shipping_fee DESC;
