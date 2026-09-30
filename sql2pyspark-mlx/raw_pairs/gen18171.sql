SELECT
    a.*,
    b.email,
    b.signup_date
FROM accounts a
JOIN customers b
    ON a.customer_id = b.id;
