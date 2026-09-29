SELECT o.channel, MIN(p.amount) AS min_amount FROM payments p JOIN orders o ON o.id = p.order_id WHERE o.store_id IS NULL GROUP BY o.channel
