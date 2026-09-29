SELECT o.channel, COUNT(*) AS cnt FROM payments p LEFT JOIN orders o ON p.order_id = o.id GROUP BY o.channel ORDER BY o.channel, cnt DESC LIMIT 10;
