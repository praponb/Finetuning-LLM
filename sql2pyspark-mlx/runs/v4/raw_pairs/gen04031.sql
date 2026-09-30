SELECT MONTH(c.signup_date) signup_month, c.city, SUM(CASE WHEN s.status = 'PAUSED' THEN 1 ELSE 0 END) paused_count
FROM subscriptions s
JOIN customers c ON s.customer_id = c.id
GROUP BY MONTH(c.signup_date), c.city;
