SELECT
    s.plan,
    s.start_date,
    c.segment,
    CASE WHEN c.city = 'Singapore' THEN 1 ELSE 0 END is_singapore
FROM subscriptions s
INNER JOIN customers c ON c.id = s.customer_id;
