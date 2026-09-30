SELECT o.id, o.status, COUNT(p.id) AS payment_count, ROUND(AVG(p.amount), 2) AS avg_amount FROM orders o LEFT JOIN payments p ON p.order_id = o.id GROUP BY o.id, o.status ORDER BY id DESC
