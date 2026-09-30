SELECT
    id,
    signup_date,
    LENGTH(city) AS city_length,
    ROUND(age, 1) AS age_rounded
FROM customers
WHERE is_active OR signup_date > '2025-03-10'
