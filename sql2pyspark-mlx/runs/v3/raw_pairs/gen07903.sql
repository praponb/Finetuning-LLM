SELECT CASE WHEN YEAR(o.order_date) < 2024 THEN 'before_2024' WHEN YEAR(o.order_date) = 2024 THEN '2024' ELSE 'after_2024' END AS order_cohort, o.channel, ROUND(AVG(s.cost), 2) AS avg_cost
FROM shipments s
INNER JOIN orders o ON s.order_id = o.id
GROUP BY CASE WHEN YEAR(o.order_date) < 2024 THEN 'before_2024' WHEN YEAR(o.order_date) = 2024 THEN '2024' ELSE 'after_2024' END, o.channel
HAVING avg_cost >= 71;
