SELECT o.channel, SUM(CASE WHEN p.status = 'SUCCESS' THEN 1 ELSE 0 END) success_count, COUNT(DISTINCT p.amount) unique_orders
FROM payments p
INNER JOIN orders o ON p.order_id = o.id
GROUP BY o.channel
HAVING COUNT(DISTINCT p.amount) > 1;
