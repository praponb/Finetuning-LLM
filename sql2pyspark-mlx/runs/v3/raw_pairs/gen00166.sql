SELECT
    id,
    start_date,
    plan,
    status
FROM subscriptions
WHERE start_date = (SELECT MAX(start_date) FROM subscriptions WHERE MONTH(start_date) IN (3, 4, 6));
