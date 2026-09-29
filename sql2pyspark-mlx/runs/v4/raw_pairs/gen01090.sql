SELECT o.order_date, o.channel, p.status
FROM orders o
INNER JOIN payments p ON p.order_id = o.id
WHERE p.status = 'SUCCESS'
ORDER BY order_date DESC;
