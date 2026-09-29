SELECT o.status, SUM(CASE WHEN p.method = 'card' THEN p.amount ELSE 0 END) AS card_amount FROM payments p LEFT JOIN orders o ON p.order_id = o.id GROUP BY o.status;
