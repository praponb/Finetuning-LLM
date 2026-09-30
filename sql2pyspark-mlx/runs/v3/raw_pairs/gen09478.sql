SELECT o.channel, COUNT(p.id) AS payment_count, MAX(p.paid_at) AS latest_paid_at
FROM orders o
LEFT JOIN payments p ON p.order_id = o.id
WHERE o.store_id IS NOT NULL
GROUP BY o.channel
ORDER BY payment_count
