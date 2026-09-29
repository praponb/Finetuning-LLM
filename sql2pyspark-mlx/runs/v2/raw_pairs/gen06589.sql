SELECT
    country,
    is_active,
    YEAR(signup_date) signup_year,
    COUNT(age) age_count,
    SUM(credit_limit) total_credit_limit
FROM customers
WHERE is_active = FALSE AND LOWER(TRIM(email)) = 'alice@gmail.com'
GROUP BY country, is_active, YEAR(signup_date)
ORDER BY signup_year DESC, country, is_active
LIMIT 10;
