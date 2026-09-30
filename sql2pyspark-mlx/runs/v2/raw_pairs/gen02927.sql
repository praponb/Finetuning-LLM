SELECT DATE_TRUNC('MONTH', o.order_date) AS order_month, ROUND(STDDEV(s.cost), 2) AS stddev_cost, SUM(s.cost) AS total_cost
FROM shipments s
LEFT OUTER JOIN orders o ON s.order_id = o.id
INNER JOIN customers c ON o.customer_id = c.id
WHERE s.carrier IN ('Kerry', 'DHL')
GROUP BY DATE_TRUNC('MONTH', o.order_date);
