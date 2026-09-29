SELECT MONTH(o.order_date) AS order_month, o.status, MAX(s.cost) - MIN(s.cost) AS cost_range
FROM shipments s
LEFT JOIN orders o ON s.order_id = o.id
WHERE o.customer_id IS NOT NULL
GROUP BY MONTH(o.order_date), o.status
