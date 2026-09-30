SELECT
    s.id,
    s.device,
    c.age,
    c.signup_date,
    c.city,
    DATE_FORMAT(s.started_at, 'yyyy-MM') AS started_at_label
FROM sessions s
INNER JOIN customers c ON c.id = s.customer_id
WHERE s.device IN ('mobile', 'desktop')
