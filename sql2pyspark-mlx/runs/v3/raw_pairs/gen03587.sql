SELECT status, CASE WHEN YEAR(order_date) < 2024 THEN 'before_2024' WHEN YEAR(order_date) = 2024 THEN '2024' ELSE 'after_2024' END AS order_cohort, COUNT(amount) AS amount_count
FROM orders
WHERE status IN ('CANCELLED', 'RETURNED')
GROUP BY 1, 2
ORDER BY order_cohort DESC, amount_count DESC
