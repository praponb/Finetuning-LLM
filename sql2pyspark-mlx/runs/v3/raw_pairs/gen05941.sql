WITH recent_customers AS (
    SELECT *, SUBSTRING(country, 1, 2) AS country_prefix
    FROM customers
    WHERE country IN ('US', 'DE') AND segment IS NOT NULL
),
agg AS (
    SELECT CASE WHEN YEAR(signup_date) < 2024 THEN 'before_2024' WHEN YEAR(signup_date) = 2024 THEN '2024' ELSE 'after_2024' END AS signup_cohort, COUNT(age) AS age_count
    FROM recent_customers
    GROUP BY CASE WHEN YEAR(signup_date) < 2024 THEN 'before_2024' WHEN YEAR(signup_date) = 2024 THEN '2024' ELSE 'after_2024' END
)
SELECT *
FROM agg
ORDER BY age_count DESC
