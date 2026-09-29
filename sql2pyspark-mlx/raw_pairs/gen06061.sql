SELECT COUNT(monthly_fee) monthly_fee_count, SUM(CASE WHEN status = 'EXPIRED' THEN monthly_fee ELSE 0 END) expired_monthly_fee, ROUND(STDDEV(monthly_fee), 2) stddev_monthly_fee
FROM subscriptions
