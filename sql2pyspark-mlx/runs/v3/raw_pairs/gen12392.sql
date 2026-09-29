SELECT o.channel, MIN(s.cost) AS min_cost
FROM shipments s
LEFT OUTER JOIN orders o ON s.order_id = o.id
RIGHT OUTER JOIN customers c ON o.customer_id = c.id
WHERE o.order_date BETWEEN '2024-01-22' AND '2024-06-01'
GROUP BY o.channel
ORDER BY channel DESC;
