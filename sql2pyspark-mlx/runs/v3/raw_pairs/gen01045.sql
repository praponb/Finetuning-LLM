SELECT o.status, SUM(s.cost) AS total_cost, SUM(CASE WHEN s.status = 'DELIVERED' THEN 1 ELSE 0 END) AS delivered_count
FROM shipments s
LEFT JOIN orders o ON s.order_id = o.id
WHERE o.status IN ('PENDING', 'COMPLETED', 'SHIPPED')
GROUP BY o.status
HAVING SUM(s.cost) >= 530
ORDER BY total_cost ASC
