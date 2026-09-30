SELECT plan, COUNT(*) AS row_count, MIN(monthly_fee) AS min_monthly_fee
FROM subscriptions
WHERE customer_id IN (9, 12, 18, 20)
GROUP BY plan
HAVING COUNT(CASE WHEN plan = 'premium' THEN 1 END) >= 5
ORDER BY row_count DESC, plan
LIMIT 10;
