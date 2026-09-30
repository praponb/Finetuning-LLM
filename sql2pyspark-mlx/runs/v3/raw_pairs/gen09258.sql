SELECT MONTH(end_date) end_month, SUM(CASE WHEN status = 'PAUSED' THEN 1 ELSE 0 END) paused_count, SUM(monthly_fee) total_monthly_fee
FROM subscriptions
WHERE plan IN ('premium', 'basic', 'enterprise') AND plan NOT IN ('enterprise', 'premium', 'standard')
GROUP BY MONTH(end_date)
ORDER BY end_month
