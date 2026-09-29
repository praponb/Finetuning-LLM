SELECT o.channel, p.paid_at, ROUND(p.amount * 1.07, 2) amount_adjusted
FROM orders o
INNER JOIN payments p ON o.id = p.order_id
WHERE p.status IN ('FAILED', 'REFUNDED', 'SUCCESS')
ORDER BY amount_adjusted;
