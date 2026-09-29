SELECT
    id,
    plan,
    monthly_fee,
    start_date,
    CONCAT(status, '/', plan) AS status_plan,
    CAST(monthly_fee AS INT) AS monthly_fee_int
FROM subscriptions
WHERE monthly_fee <= 120 OR plan = 'basic';
