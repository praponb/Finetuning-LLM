SELECT status, MAX(monthly_fee) AS max_monthly_fee, SUM(CASE WHEN status = 'PAUSED' THEN monthly_fee ELSE 0 END) AS paused_monthly_fee FROM subscriptions GROUP BY status HAVING MAX(monthly_fee) < 160
