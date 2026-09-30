SELECT
    method,
    CASE WHEN YEAR(paid_at) < 2024 THEN 'before_2024' WHEN YEAR(paid_at) = 2024 THEN '2024' ELSE 'after_2024' END AS paid_cohort,
    COUNT(CASE WHEN method = 'cash' THEN 1 END) AS cash_count,
    COUNT(amount) AS amount_count,
    COUNT(CASE WHEN status = 'FAILED' THEN 1 END) AS failed_count
FROM payments
WHERE paid_at < '2024-05-01'
GROUP BY method, paid_cohort
HAVING COUNT(CASE WHEN method = 'cash' THEN 1 END) >= 5;
