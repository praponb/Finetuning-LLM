SELECT
    DATE_FORMAT(o.order_date, 'yyyy-MM') AS order_month,
    o.status,
    SUM(CASE WHEN s.status = 'DELIVERED' THEN 1 ELSE 0 END) AS delivered_count,
    MAX(s.cost) AS max_cost
FROM shipments s
RIGHT JOIN orders o ON s.order_id = o.id
WHERE s.status IN ('LOST', 'RETURNED')
GROUP BY DATE_FORMAT(o.order_date, 'yyyy-MM'), o.status
ORDER BY status DESC, order_month
LIMIT 10;
