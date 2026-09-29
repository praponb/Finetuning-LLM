SELECT
    method,
    CASE WHEN YEAR(paid_at) < 2024 THEN 'before_2024' WHEN YEAR(paid_at) = 2024 THEN '2024' ELSE 'after_2024' END AS paid_cohort,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(method))) AS method_values,
    ROUND(AVG(amount), 2) AS avg_amount
FROM payments
GROUP BY 1, 2
HAVING ROUND(AVG(amount), 2) < 43
