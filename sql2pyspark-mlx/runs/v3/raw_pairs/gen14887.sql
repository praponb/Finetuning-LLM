SELECT plan, customer_id, ROUND(monthly_fee * 1.15, 2) AS monthly_fee_scaled
FROM subscriptions
ORDER BY monthly_fee_scaled
