SELECT
    id,
    name,
    LOWER(name) AS name_lower,
    DATE_SUB(signup_date, 12) AS signup_date_minus_12d
FROM customers
WHERE MONTH(signup_date) IN (1, 3, 8) AND signup_date >= '2023-10-01' AND LOWER(TRIM(name)) = 'emma brown';
