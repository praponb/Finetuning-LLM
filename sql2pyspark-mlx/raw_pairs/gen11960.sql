SELECT
    we.duration_sec,
    we.session_id,
    s.country,
    c.city
FROM web_events we
RIGHT OUTER JOIN sessions s ON we.session_id = s.id
JOIN customers c ON s.customer_id = c.id
WHERE c.signup_date <= '2024-10-01' AND s.device = 'mobile'
ORDER BY city;
