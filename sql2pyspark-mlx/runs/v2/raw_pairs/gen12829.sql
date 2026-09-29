SELECT id, monthly_fee
FROM subscriptions
WHERE monthly_fee >= (SELECT AVG(monthly_fee) FROM subscriptions WHERE status = 'EXPIRED')
ORDER BY monthly_fee DESC;
