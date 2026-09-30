SELECT status, COUNT(*) AS row_count
FROM subscriptions
WHERE monthly_fee < 200 AND status IN ('CANCELLED', 'ACTIVE')
GROUP BY status;
