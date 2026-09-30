SELECT
    s.id,
    s.device,
    s.customer_id,
    s.started_at,
    c.email,
    DAYOFWEEK(c.signup_date) AS signup_date_dayofweek
FROM sessions s
INNER JOIN customers c ON c.id = s.customer_id;
