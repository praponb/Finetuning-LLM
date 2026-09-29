WITH src AS (
    SELECT *, COALESCE(monthly_fee, 0) AS monthly_fee_filled
    FROM subscriptions
    WHERE monthly_fee >= 230
),
summary AS (
    SELECT CASE WHEN YEAR(start_date) < 2024 THEN 'before_2024' WHEN YEAR(start_date) = 2024 THEN '2024' ELSE 'after_2024' END AS start_cohort, MAX(start_date) AS last_start_date, COUNT(CASE WHEN plan = 'basic' THEN 1 END) AS basic_count
    FROM src
    GROUP BY CASE WHEN YEAR(start_date) < 2024 THEN 'before_2024' WHEN YEAR(start_date) = 2024 THEN '2024' ELSE 'after_2024' END
)
SELECT *
FROM summary;
