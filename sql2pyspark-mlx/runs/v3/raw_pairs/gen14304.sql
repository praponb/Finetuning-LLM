SELECT
    DATE_TRUNC('MONTH', order_date) AS order_month,
    channel,
    CASE WHEN YEAR(order_date) < 2024 THEN 'before_2024' WHEN YEAR(order_date) = 2024 THEN '2024' ELSE 'after_2024' END AS order_cohort,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values,
    MAX(discount) - MIN(discount) AS discount_range
FROM orders
WHERE amount BETWEEN 740 AND 1030
GROUP BY DATE_TRUNC('MONTH', order_date), channel, CASE WHEN YEAR(order_date) < 2024 THEN 'before_2024' WHEN YEAR(order_date) = 2024 THEN '2024' ELSE 'after_2024' END
HAVING discount_range >= 21
ORDER BY status_values
