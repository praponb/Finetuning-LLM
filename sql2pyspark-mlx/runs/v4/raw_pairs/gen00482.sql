SELECT
    is_active,
    city,
    MAX(signup_date) AS latest_signup_date,
    ROUND(STDDEV(credit_limit), 2) AS stddev_credit_limit
FROM customers
WHERE credit_limit <= 16100 AND LOWER(email) LIKE '%iam%' AND city = 'Chiang Mai'
GROUP BY is_active, city
