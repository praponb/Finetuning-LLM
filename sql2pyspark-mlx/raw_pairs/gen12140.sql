WITH RecentPayments AS (
    SELECT *, CEIL(amount) AS amount_whole
    FROM payments
    WHERE HOUR(paid_at) BETWEEN 6 AND 13 AND amount >= 1000
),
payments_stats AS (
    SELECT CASE WHEN YEAR(paid_at) < 2024 THEN 'before_2024' WHEN YEAR(paid_at) = 2024 THEN '2024' ELSE 'after_2024' END AS paid_cohort, COUNT(amount) AS amount_count
    FROM RecentPayments
    GROUP BY CASE WHEN YEAR(paid_at) < 2024 THEN 'before_2024' WHEN YEAR(paid_at) = 2024 THEN '2024' ELSE 'after_2024' END
)
SELECT *
FROM payments_stats
ORDER BY paid_cohort DESC;
