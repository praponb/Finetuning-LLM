SELECT
    c.city,
    c.age,
    c.signup_date,
    s.browser,
    s.customer_id,
    LAST_DAY(c.signup_date) AS signup_date_month_end
FROM customers c
JOIN sessions s ON c.id = s.customer_id
WHERE c.country = 'US'
ORDER BY 5;
