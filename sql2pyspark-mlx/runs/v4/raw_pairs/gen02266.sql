WITH FilteredPayments AS (
    SELECT *
    FROM payments
    WHERE amount >= 1310
),
paid_cohort_summary AS (
    SELECT CASE WHEN YEAR(paid_at) < 2024 THEN 'before_2024' WHEN YEAR(paid_at) = 2024 THEN '2024' ELSE 'after_2024' END AS paid_cohort, status, MIN(amount) AS min_amount
    FROM FilteredPayments
    GROUP BY CASE WHEN YEAR(paid_at) < 2024 THEN 'before_2024' WHEN YEAR(paid_at) = 2024 THEN '2024' ELSE 'after_2024' END, status
)
SELECT *
FROM paid_cohort_summary
ORDER BY min_amount DESC, paid_cohort, status
LIMIT 5
