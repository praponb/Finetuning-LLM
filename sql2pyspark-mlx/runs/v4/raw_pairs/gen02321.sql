SELECT id, status, end_date, ROUND(monthly_fee * 1.1, 2) AS monthly_fee_adjusted FROM subscriptions WHERE monthly_fee >= 80 AND plan = 'standard' AND status = 'PAUSED' ORDER BY status DESC
