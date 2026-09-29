SELECT id, monthly_fee, start_date
FROM subscriptions
WHERE monthly_fee = (SELECT MIN(monthly_fee) FROM subscriptions)
ORDER BY monthly_fee DESC;
