WITH valid_customers AS (
    SELECT *
    FROM customers
    WHERE age > 43 AND UPPER(email) LIKE '%RAC%'
),
summary AS (
    SELECT CASE WHEN YEAR(signup_date) < 2024 THEN 'before_2024' WHEN YEAR(signup_date) = 2024 THEN '2024' ELSE 'after_2024' END AS signup_cohort, SUM(CASE WHEN city = 'Bangkok' THEN 1 ELSE 0 END) AS bangkok_count
    FROM valid_customers
    GROUP BY CASE WHEN YEAR(signup_date) < 2024 THEN 'before_2024' WHEN YEAR(signup_date) = 2024 THEN '2024' ELSE 'after_2024' END
)
SELECT signup_cohort, bangkok_count
FROM summary;
