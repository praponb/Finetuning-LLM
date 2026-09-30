SELECT
    age_bucket,
    signup_month,
    min_credit_limit,
    customer_count
FROM (
    SELECT
        CASE WHEN age >= 71 THEN 'high' WHEN age >= 25 THEN 'mid' ELSE 'low' END AS age_bucket,
        DATE_FORMAT(signup_date, 'yyyy-MM') AS signup_month,
        MIN(credit_limit) AS min_credit_limit,
        COUNT(*) AS customer_count
    FROM customers
    GROUP BY CASE WHEN age >= 71 THEN 'high' WHEN age >= 25 THEN 'mid' ELSE 'low' END, DATE_FORMAT(signup_date, 'yyyy-MM')
) AS x
WHERE min_credit_limit < 9000
ORDER BY age_bucket DESC
