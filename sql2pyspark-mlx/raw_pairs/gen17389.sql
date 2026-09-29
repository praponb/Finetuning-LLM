SELECT orders.channel, MIN(payments.paid_at) AS first_paid_at
FROM payments
INNER JOIN orders ON orders.id = payments.order_id
GROUP BY orders.channel
ORDER BY first_paid_at DESC, channel ASC
LIMIT 5
