SELECT
    s.id,
    s.started_at,
    s.device,
    s.customer_id,
    c.signup_date,
    c.credit_limit,
    c.age,
    QUARTER(c.signup_date) AS signup_date_quarter
FROM sessions s
FULL OUTER JOIN customers c ON s.customer_id = c.id
