SELECT p.status, o.channel, o.status AS order_status
FROM payments p
FULL OUTER JOIN orders o ON p.order_id = o.id
WHERE o.amount >= 1320
ORDER BY o.channel DESC;
