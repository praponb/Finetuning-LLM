SELECT MONTH(signup_date) AS signup_month, segment, MAX(signup_date) AS latest_signup_date
FROM customers
WHERE age IS NOT NULL AND email LIKE '%@company.co' AND email IS NULL
GROUP BY CUBE(MONTH(signup_date), segment);
