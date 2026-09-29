SELECT
    c.city,
    c.signup_date,
    c.name,
    o.amount,
    DATEDIFF(CURRENT_DATE(), c.signup_date) AS days_since_signup_date
FROM customers c
LEFT JOIN orders o ON c.id = o.customer_id
WHERE c.signup_date < '2023-03-01' OR o.discount <= 0.21
ORDER BY signup_date DESC, o.amount DESC;
