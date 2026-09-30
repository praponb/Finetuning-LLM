SELECT o.channel, MIN(s.delivered_date) AS first_delivered_date
FROM shipments s
INNER JOIN orders o ON o.id = s.order_id
INNER JOIN stores st ON o.store_id = st.id
WHERE s.delivered_date > '2025-01-01'
GROUP BY o.channel
ORDER BY channel ASC
LIMIT 50
