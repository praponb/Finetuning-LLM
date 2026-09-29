SELECT o.store_id, MAX(p.paid_at) AS last_paid_at FROM payments p JOIN orders o ON p.order_id = o.id WHERE p.method <> 'card' GROUP BY o.store_id ORDER BY store_id LIMIT 50;
