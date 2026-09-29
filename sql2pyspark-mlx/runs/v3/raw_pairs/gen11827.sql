SELECT
    channel,
    customer_id,
    CASE WHEN YEAR(order_date) < 2024 THEN 'before_2024' WHEN YEAR(order_date) = 2024 THEN '2024' ELSE 'after_2024' END AS order_cohort,
    MAX(discount) AS max_discount
FROM orders
WHERE discount < 0.21
GROUP BY 1, 2, 3
HAVING MAX(discount) >= 0.38
ORDER BY customer_id
