SELECT plan, CASE WHEN YEAR(end_date) < 2024 THEN 'before_2024' WHEN YEAR(end_date) = 2024 THEN '2024' ELSE 'after_2024' END AS end_cohort, MAX(end_date) AS last_end_date
FROM subscriptions
WHERE end_date BETWEEN '2024-01-01' AND '2025-03-08' AND end_date BETWEEN '2024-09-01' AND '2025-01-07'
GROUP BY plan, end_cohort;
