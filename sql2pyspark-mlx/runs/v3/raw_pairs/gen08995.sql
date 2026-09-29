SELECT CASE WHEN YEAR(order_date) < 2024 THEN 'before_2024' WHEN YEAR(order_date) = 2024 THEN '2024' ELSE 'after_2024' END AS order_cohort, channel, MAX(amount) - MIN(amount) AS amount_range
FROM orders
GROUP BY 1, 2
ORDER BY channel, order_cohort
LIMIT 3
