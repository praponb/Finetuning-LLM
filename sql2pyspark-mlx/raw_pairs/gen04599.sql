SELECT o.status, o.channel, MAX(p.amount) AS max_amount FROM payments p LEFT JOIN orders o ON p.order_id = o.id GROUP BY o.status, o.channel ORDER BY 1;
