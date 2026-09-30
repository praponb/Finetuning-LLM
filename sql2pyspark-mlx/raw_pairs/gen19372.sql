SELECT *, ROUND(monthly_fee * 100, 2) AS monthly_fee_adjusted, TRIM(status) AS clean_status FROM subscriptions WHERE status IN ('CANCELLED', 'PAUSED', 'EXPIRED') ORDER BY plan, id LIMIT 20
