SELECT
    a.account_type,
    a.customer_id,
    c.signup_date,
    ROUND(c.age, 2) age_rounded
FROM accounts a
LEFT OUTER JOIN customers c ON a.customer_id = c.id;
