SELECT
    monthly_fee,
    customer_id,
    status,
    LOWER(status) AS status_lower
FROM subscriptions
WHERE customer_id = 11 AND (monthly_fee > 180 OR status = 'PAUSED');
