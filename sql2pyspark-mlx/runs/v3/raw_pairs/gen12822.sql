SELECT CASE WHEN YEAR(paid_at) < 2024 THEN 'before_2024' WHEN YEAR(paid_at) = 2024 THEN '2024' ELSE 'after_2024' END AS paid_cohort, ROUND(AVG(amount), 2) AS avg_amount
FROM payments
WHERE order_id IS NULL AND status IN ('REFUNDED', 'FAILED')
GROUP BY paid_cohort
ORDER BY paid_cohort DESC;
