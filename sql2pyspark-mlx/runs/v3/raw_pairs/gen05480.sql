SELECT s.id, s.start_date, c.segment
FROM subscriptions s
JOIN customers c ON c.id = s.customer_id
WHERE YEAR(c.signup_date) = 2025 OR s.status != 'CANCELLED'
ORDER BY id DESC
