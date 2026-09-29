SELECT *, CONCAT(status, ' - ', plan) AS status_plan
FROM subscriptions
WHERE status IN ('CANCELLED', 'PAUSED')
ORDER BY monthly_fee DESC;
