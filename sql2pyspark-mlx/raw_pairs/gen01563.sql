SELECT
    status,
    start_date,
    plan,
    DATEDIFF(CURRENT_DATE(), end_date) / 365.25 AS years_since_end_date
FROM subscriptions
WHERE plan <> 'enterprise';
