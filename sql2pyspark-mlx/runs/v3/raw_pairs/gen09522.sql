SELECT
    CASE WHEN credit_limit >= 13800 THEN 'high' WHEN credit_limit >= 4000 THEN 'mid' ELSE 'low' END AS credit_limit_bucket,
    country,
    MAX(signup_date) AS latest_signup_date,
    SUM(credit_limit) AS total_credit_limit
FROM customers
WHERE name LIKE 'Gra%' AND credit_limit >= 8400
GROUP BY CASE WHEN credit_limit >= 13800 THEN 'high' WHEN credit_limit >= 4000 THEN 'mid' ELSE 'low' END, country
HAVING total_credit_limit < 62000;
