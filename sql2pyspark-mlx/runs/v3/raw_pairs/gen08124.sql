SELECT o.discount, o.customer_id, p.amount
FROM orders o
LEFT JOIN payments p ON p.order_id = o.id
WHERE p.order_id IN (7, 25, 31, 35) OR p.paid_at < '2024-12-01'
ORDER BY discount DESC, p.amount DESC
