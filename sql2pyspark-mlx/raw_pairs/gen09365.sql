SELECT
    id,
    segment,
    LENGTH(email) AS email_length,
    ADD_MONTHS(signup_date, 6) AS signup_date_plus_months
FROM customers
WHERE name LIKE 'Ivy%'
ORDER BY signup_date_plus_months, segment DESC;
