SELECT
    QUARTER(signup_date) AS signup_quarter,
    city,
    COUNT(CASE WHEN country = 'DE' THEN 1 END) AS de_count,
    COUNT(*) AS customer_count
FROM customers
WHERE LENGTH(email) < 10
GROUP BY QUARTER(signup_date), city
ORDER BY customer_count DESC, signup_quarter, city
LIMIT 5
