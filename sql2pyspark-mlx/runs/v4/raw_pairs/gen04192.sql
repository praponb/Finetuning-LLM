SELECT
    orders.channel,
    CASE WHEN YEAR(orders.order_date) < 2024 THEN 'before_2024' WHEN YEAR(orders.order_date) = 2024 THEN '2024' ELSE 'after_2024' END AS order_cohort,
    COUNT(shipments.cost) AS cost_count,
    AVG(shipments.cost) AS avg_cost
FROM shipments
LEFT OUTER JOIN orders ON shipments.order_id = orders.id
WHERE orders.customer_id IN (8, 13, 22)
GROUP BY orders.channel, CASE WHEN YEAR(orders.order_date) < 2024 THEN 'before_2024' WHEN YEAR(orders.order_date) = 2024 THEN '2024' ELSE 'after_2024' END
HAVING avg_cost > 52
