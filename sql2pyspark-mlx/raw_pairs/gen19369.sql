SELECT QUARTER(o.order_date) AS order_quarter, AVG(s.cost) AS avg_cost, SUM(CASE WHEN s.status = 'LOST' THEN s.cost ELSE 0 END) AS lost_cost
FROM shipments s
RIGHT OUTER JOIN orders o ON s.order_id = o.id
WHERE o.channel = 'mobile'
GROUP BY QUARTER(o.order_date)
HAVING AVG(s.cost) > 81
ORDER BY lost_cost
