SELECT o.id, o.discount, o.shipping_fee
FROM orders o
WHERE o.customer_id IN (SELECT id FROM customers WHERE credit_limit < 4900)
ORDER BY o.id
