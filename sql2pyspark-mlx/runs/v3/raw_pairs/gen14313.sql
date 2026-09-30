SELECT CASE WHEN YEAR(o.order_date) < 2024 THEN 'before_2024' WHEN YEAR(o.order_date) = 2024 THEN '2024' ELSE 'after_2024' END AS order_cohort, COUNT(DISTINCT oi.unit_price) AS unique_orders
FROM order_items oi
LEFT JOIN orders o ON oi.order_id = o.id
INNER JOIN products p ON oi.product_id = p.id
GROUP BY CASE WHEN YEAR(o.order_date) < 2024 THEN 'before_2024' WHEN YEAR(o.order_date) = 2024 THEN '2024' ELSE 'after_2024' END
ORDER BY unique_orders DESC, order_cohort DESC
LIMIT 50;
