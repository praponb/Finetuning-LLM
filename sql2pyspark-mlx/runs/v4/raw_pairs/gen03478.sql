WITH valid_payments AS (
    SELECT *
    FROM payments
    WHERE order_id IS NOT NULL AND paid_at < '2023-09-01' AND order_id IS NOT NULL
),
agg AS (
    SELECT order_id, CASE WHEN YEAR(paid_at) < 2024 THEN 'before_2024' WHEN YEAR(paid_at) = 2024 THEN '2024' ELSE 'after_2024' END AS paid_cohort, ROUND((100 * SUM(CASE WHEN status = 'REFUNDED' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_refunded
    FROM valid_payments
    GROUP BY order_id, CASE WHEN YEAR(paid_at) < 2024 THEN 'before_2024' WHEN YEAR(paid_at) = 2024 THEN '2024' ELSE 'after_2024' END
)
SELECT order_id, paid_cohort
FROM agg
WHERE pct_refunded > 45
ORDER BY paid_cohort;
