SELECT SUM(CASE WHEN status = 'ACTIVE' THEN monthly_fee ELSE 0 END) active_monthly_fee, MAX(monthly_fee) - MIN(monthly_fee) monthly_fee_range
FROM subscriptions
WHERE MONTH(start_date) IN (4, 5, 8)
ORDER BY active_monthly_fee, monthly_fee_range
