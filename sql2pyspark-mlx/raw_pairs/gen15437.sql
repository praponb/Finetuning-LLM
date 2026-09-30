SELECT o.id, o.order_date, o.customer_id FROM orders o WHERE o.discount < 0.36 AND o.customer_id NOT IN (SELECT id FROM customers WHERE country IN ('TH', 'JP')) ORDER BY order_date DESC
