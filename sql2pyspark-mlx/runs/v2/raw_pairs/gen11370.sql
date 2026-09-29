SELECT
    id,
    credit_limit,
    name,
    DATE_FORMAT(signup_date, 'yyyy-MM') AS signup_date_label,
    DATEDIFF(CURRENT_DATE(), signup_date) AS days_since_signup_date
FROM customers
WHERE email IS NOT NULL AND name LIKE 'No%' AND LENGTH(email) > 8
ORDER BY name, id
LIMIT 20
