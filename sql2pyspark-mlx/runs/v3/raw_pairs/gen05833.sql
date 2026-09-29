SELECT o.channel, MAX(s.delivered_date) AS last_delivered_date
FROM shipments s
INNER JOIN orders o ON s.order_id = o.id
WHERE MONTH(o.order_date) IN (1, 3, 6)
GROUP BY o.channel
ORDER BY 2 DESC;
