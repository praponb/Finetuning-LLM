SELECT
    signup_cohort,
    segment,
    pct_new_york,
    customer_count
FROM (
    SELECT
        CASE WHEN YEAR(signup_date) < 2024 THEN 'before_2024' WHEN YEAR(signup_date) = 2024 THEN '2024' ELSE 'after_2024' END AS signup_cohort,
        segment,
        ROUND((100 * SUM(CASE WHEN city = 'New York' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_new_york,
        COUNT(*) AS customer_count
    FROM customers
    GROUP BY CASE WHEN YEAR(signup_date) < 2024 THEN 'before_2024' WHEN YEAR(signup_date) = 2024 THEN '2024' ELSE 'after_2024' END, segment
) AS s
WHERE customer_count >= 8
ORDER BY customer_count DESC;
