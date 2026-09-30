SELECT o.id, o.order_date
FROM orders o
WHERE o.customer_id IN (SELECT id FROM customers WHERE country != 'DE')
ORDER BY order_date, o.id DESC;
