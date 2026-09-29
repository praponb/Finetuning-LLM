SELECT *
FROM (
    SELECT status, CASE WHEN YEAR(order_date) < 2024 THEN 'before_2024' WHEN YEAR(order_date) = 2024 THEN '2024' ELSE 'after_2024' END AS order_cohort, COUNT(*) AS total_count
    FROM orders
    WHERE customer_id IS NOT NULL
    GROUP BY status, CASE WHEN YEAR(order_date) < 2024 THEN 'before_2024' WHEN YEAR(order_date) = 2024 THEN '2024' ELSE 'after_2024' END
) AS sub
WHERE total_count > 5
ORDER BY order_cohort, status
LIMIT 50
