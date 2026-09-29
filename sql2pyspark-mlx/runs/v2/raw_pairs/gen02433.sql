SELECT
    id,
    plan,
    status,
    LAST_DAY(end_date) AS end_date_month_end
FROM subscriptions
WHERE status = 'CANCELLED'
