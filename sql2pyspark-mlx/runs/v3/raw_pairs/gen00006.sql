SELECT *
FROM (
    SELECT
        is_active,
        CASE WHEN YEAR(signup_date) < 2024 THEN 'before_2024' WHEN YEAR(signup_date) = 2024 THEN '2024' ELSE 'after_2024' END AS signup_cohort,
        MIN(credit_limit) AS min_credit_limit,
        SUM(credit_limit) AS total_credit_limit
    FROM customers
    WHERE email IS NOT NULL
    GROUP BY is_active, CASE WHEN YEAR(signup_date) < 2024 THEN 'before_2024' WHEN YEAR(signup_date) = 2024 THEN '2024' ELSE 'after_2024' END
) AS agg
WHERE min_credit_limit >= 14400
ORDER BY min_credit_limit DESC;
