SELECT *
FROM (
    SELECT method, CASE WHEN YEAR(paid_at) < 2024 THEN 'before_2024' WHEN YEAR(paid_at) = 2024 THEN '2024' ELSE 'after_2024' END AS paid_cohort, ROUND(AVG(amount), 2) AS avg_amount
    FROM payments
    GROUP BY method, CASE WHEN YEAR(paid_at) < 2024 THEN 'before_2024' WHEN YEAR(paid_at) = 2024 THEN '2024' ELSE 'after_2024' END
) AS t
WHERE avg_amount > 47
ORDER BY paid_cohort;
