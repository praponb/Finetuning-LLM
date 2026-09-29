SELECT
    start_cohort,
    plan,
    expired_count,
    active_count
FROM (
    SELECT
        CASE WHEN YEAR(start_date) < 2024 THEN 'before_2024' WHEN YEAR(start_date) = 2024 THEN '2024' ELSE 'after_2024' END AS start_cohort,
        plan,
        SUM(CASE WHEN status = 'EXPIRED' THEN 1 ELSE 0 END) AS expired_count,
        COUNT(CASE WHEN status = 'ACTIVE' THEN 1 END) AS active_count
    FROM subscriptions
    WHERE plan = 'premium'
    GROUP BY CASE WHEN YEAR(start_date) < 2024 THEN 'before_2024' WHEN YEAR(start_date) = 2024 THEN '2024' ELSE 'after_2024' END, plan
) AS t
WHERE active_count >= 6;
