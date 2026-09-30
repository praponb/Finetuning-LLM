SELECT o.status, p.amount
FROM orders o
LEFT JOIN payments p ON p.order_id = o.id
WHERE p.status = 'FAILED' OR p.status = 'FAILED'
ORDER BY amount DESC;
