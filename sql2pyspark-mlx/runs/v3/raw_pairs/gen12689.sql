SELECT
    country,
    CASE WHEN credit_limit >= 16300 THEN 'high' WHEN credit_limit >= 3900 THEN 'mid' ELSE 'low' END AS credit_limit_bucket,
    DATE_FORMAT(signup_date, 'yyyy-MM') AS signup_month,
    MAX(signup_date) AS last_signup_date
FROM customers
WHERE city != 'Berlin' AND age <= 42
GROUP BY 1, 2, 3;
