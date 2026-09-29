SELECT
    YEAR(o.order_date) AS order_year,
    MONTH(o.order_date) AS order_month,
    COUNT(*) AS row_count,
    SUM(s.cost) AS total_cost
FROM shipments s
LEFT OUTER JOIN orders o ON s.order_id = o.id
WHERE s.status = 'RETURNED'
GROUP BY YEAR(o.order_date), MONTH(o.order_date)
HAVING total_cost >= 460
