SELECT
    id,
    signup_date,
    age,
    DATE_ADD(signup_date, 81) AS signup_date_plus_81d
FROM customers
WHERE is_active = TRUE
ORDER BY age DESC, signup_date DESC;
