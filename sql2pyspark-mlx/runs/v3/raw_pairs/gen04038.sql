SELECT COUNT(CASE WHEN plan = 'premium' THEN 1 END) premium_count
FROM subscriptions
WHERE monthly_fee BETWEEN 160 AND 220
ORDER BY premium_count DESC
