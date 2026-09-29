SELECT
    plan,
    status,
    DATEDIFF(end_date, start_date) AS days_start_date_to_end_date,
    ROUND(monthly_fee, 2) AS monthly_fee_rounded
FROM subscriptions
WHERE status IN ('CANCELLED', 'PAUSED') AND end_date >= '2023-09-09' AND status IN ('PAUSED', 'EXPIRED', 'ACTIVE');
