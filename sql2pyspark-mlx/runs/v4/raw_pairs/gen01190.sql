SELECT o.id, o.channel, COUNT(p.id) AS payment_count, COUNT(p.amount) AS amount_count FROM orders o INNER JOIN payments p ON o.id = p.order_id GROUP BY o.id, o.channel ORDER BY payment_count;
