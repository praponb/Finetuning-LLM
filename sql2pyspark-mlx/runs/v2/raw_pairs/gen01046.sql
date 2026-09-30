SELECT o.customer_id, ROUND(STDDEV(p.amount), 2) AS stddev_amount, MAX(p.paid_at) AS last_paid_at
FROM payments p
INNER JOIN orders o ON o.id = p.order_id
WHERE o.discount >= 0.34
GROUP BY o.customer_id
ORDER BY 1, last_paid_at
LIMIT 5
