SELECT
    id,
    email,
    signup_date,
    CASE WHEN age >= 73 THEN 'High' WHEN age >= 26 THEN 'Medium' ELSE 'Low' END AS age_level
FROM customers
