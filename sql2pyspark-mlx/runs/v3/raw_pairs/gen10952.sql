SELECT COUNT(monthly_fee) AS monthly_fee_count
FROM subscriptions
WHERE status = 'PAUSED' AND end_date >= '2025-03-01';
