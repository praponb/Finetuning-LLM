SELECT o.id, o.customer_id FROM orders o WHERE o.id IN (SELECT order_id FROM payments WHERE method IN ('card', 'promptpay')) ORDER BY customer_id DESC;
