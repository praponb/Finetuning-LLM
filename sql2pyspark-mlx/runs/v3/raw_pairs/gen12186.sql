SELECT QUARTER(o.order_date) AS order_quarter, SUM(CASE WHEN s.status = 'LOST' THEN s.cost ELSE 0 END) AS lost_cost
FROM shipments s
LEFT OUTER JOIN orders o ON s.order_id = o.id
GROUP BY QUARTER(o.order_date)
ORDER BY lost_cost DESC, 1 DESC
LIMIT 1;
