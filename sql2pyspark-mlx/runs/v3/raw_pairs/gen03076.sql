WITH ValidCustomers AS (
    SELECT *
    FROM customers
    WHERE city IN ('New York', 'Bangkok', 'Berlin')
),
summary AS (
    SELECT
        is_active,
        CASE WHEN YEAR(signup_date) < 2024 THEN 'before_2024' WHEN YEAR(signup_date) = 2024 THEN '2024' ELSE 'after_2024' END AS signup_cohort,
        AVG(credit_limit) AS avg_credit_limit,
        MAX(signup_date) AS latest_signup_date
    FROM ValidCustomers
    GROUP BY is_active, CASE WHEN YEAR(signup_date) < 2024 THEN 'before_2024' WHEN YEAR(signup_date) = 2024 THEN '2024' ELSE 'after_2024' END
)
SELECT
    is_active,
    signup_cohort,
    avg_credit_limit,
    latest_signup_date
FROM summary
WHERE avg_credit_limit < 9300
