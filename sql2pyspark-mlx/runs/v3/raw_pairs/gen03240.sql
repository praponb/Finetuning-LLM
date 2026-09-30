SELECT o.channel, MIN(p.paid_at) AS min_paid_at
FROM payments p
JOIN orders o ON p.order_id = o.id
GROUP BY o.channel;
