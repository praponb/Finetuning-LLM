SELECT o.channel, o.status, SUM(CASE WHEN s.status = 'RETURNED' THEN s.cost ELSE 0 END) AS returned_cost
FROM shipments s
LEFT OUTER JOIN orders o ON s.order_id = o.id
LEFT OUTER JOIN stores st ON o.store_id = st.id
GROUP BY o.channel, o.status
ORDER BY returned_cost;
