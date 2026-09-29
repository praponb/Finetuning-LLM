SELECT
    MONTH(o.order_date) AS order_month,
    YEAR(o.order_date) AS order_year,
    SUM(s.cost) AS total_cost,
    AVG(s.cost) AS avg_cost
FROM shipments s
INNER JOIN orders o ON o.id = s.order_id
GROUP BY MONTH(o.order_date), YEAR(o.order_date)
ORDER BY avg_cost DESC, order_month, order_year
LIMIT 5
