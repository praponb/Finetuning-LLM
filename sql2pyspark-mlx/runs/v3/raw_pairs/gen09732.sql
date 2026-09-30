SELECT
    c.city,
    DATE_FORMAT(c.signup_date, 'yyyy-MM') AS signup_month,
    MIN(we.duration_sec) AS min_duration_sec,
    SUM(CASE WHEN we.page = '/checkout' THEN 1 ELSE 0 END) AS checkout_count
FROM web_events we
JOIN sessions s ON s.id = we.session_id
JOIN customers c ON s.customer_id = c.id
GROUP BY c.city, DATE_FORMAT(c.signup_date, 'yyyy-MM')
