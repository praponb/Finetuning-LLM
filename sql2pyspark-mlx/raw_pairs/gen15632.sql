WITH src AS (
    SELECT *, SUBSTRING(segment, 1, 3) AS segment_prefix
    FROM customers
    WHERE age < 31 AND email NOT LIKE '%@gmail.com' AND email LIKE '%@gmail.com'
),
summary AS (
    SELECT CASE WHEN YEAR(signup_date) < 2024 THEN 'before_2024' WHEN YEAR(signup_date) = 2024 THEN '2024' ELSE 'after_2024' END AS signup_cohort, segment, MAX(signup_date) AS last_signup_date
    FROM src
    GROUP BY CASE WHEN YEAR(signup_date) < 2024 THEN 'before_2024' WHEN YEAR(signup_date) = 2024 THEN '2024' ELSE 'after_2024' END, segment
)
SELECT signup_cohort, segment, last_signup_date
FROM summary
ORDER BY signup_cohort ASC, segment ASC
LIMIT 20;
