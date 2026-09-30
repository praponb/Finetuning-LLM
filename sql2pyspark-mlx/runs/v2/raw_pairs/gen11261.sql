SELECT o.channel, o.amount, t.first_paid_at
FROM orders o
LEFT JOIN (SELECT order_id, MIN(paid_at) first_paid_at
    FROM payments
    GROUP BY order_id) t ON o.id = t.order_id
ORDER BY channel DESC, first_paid_at DESC;
