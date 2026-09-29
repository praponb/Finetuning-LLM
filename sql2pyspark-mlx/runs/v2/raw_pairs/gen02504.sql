SELECT
    country,
    signup_date,
    credit_limit,
    UPPER(country) AS country_upper
FROM customers
WHERE is_active = TRUE AND country IN ('UK', 'TH', 'SG')
