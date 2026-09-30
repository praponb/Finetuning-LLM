SELECT o.channel, o.status, MAX(s.cost) AS max_cost
FROM shipments s
INNER JOIN orders o ON s.order_id = o.id
WHERE s.carrier IN ('FedEx', 'Flash')
GROUP BY o.channel, o.status
ORDER BY status ASC;
