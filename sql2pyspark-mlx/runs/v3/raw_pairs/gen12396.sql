SELECT orders.channel, COUNT(CASE WHEN payments.method = 'card' THEN 1 END) AS card_count FROM payments INNER JOIN orders ON payments.order_id = orders.id GROUP BY orders.channel
