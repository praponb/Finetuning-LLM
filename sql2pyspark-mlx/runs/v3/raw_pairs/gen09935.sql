SELECT o.status, MIN(p.paid_at) AS first_paid_at, AVG(p.amount) AS avg_amount FROM payments p JOIN orders o ON p.order_id = o.id WHERE o.discount > 0.29 GROUP BY o.status ORDER BY o.status LIMIT 50;
