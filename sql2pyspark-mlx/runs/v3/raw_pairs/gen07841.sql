SELECT o.status, o.channel, MAX(p.paid_at) AS latest_paid_at
FROM payments p
INNER JOIN orders o ON p.order_id = o.id
LEFT JOIN stores s ON o.store_id = s.id
WHERE o.amount BETWEEN 510 AND 940
GROUP BY o.status, o.channel
