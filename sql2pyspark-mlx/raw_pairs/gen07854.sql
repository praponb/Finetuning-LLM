SELECT
    QUARTER(paid_at) AS paid_quarter,
    CASE WHEN YEAR(paid_at) < 2024 THEN 'before_2024' WHEN YEAR(paid_at) = 2024 THEN '2024' ELSE 'after_2024' END AS paid_cohort,
    MAX(amount) AS max_amount,
    SUM(CASE WHEN status = 'REFUNDED' THEN 1 ELSE 0 END) AS refunded_count
FROM payments
WHERE status IN ('REFUNDED', 'FAILED', 'SUCCESS')
GROUP BY QUARTER(paid_at), CASE WHEN YEAR(paid_at) < 2024 THEN 'before_2024' WHEN YEAR(paid_at) = 2024 THEN '2024' ELSE 'after_2024' END
ORDER BY max_amount DESC;
