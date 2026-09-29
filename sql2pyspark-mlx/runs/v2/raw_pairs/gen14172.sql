SELECT id, start_date, status, monthly_fee, ROUND(monthly_fee * 1.07, 2) AS monthly_fee_scaled FROM subscriptions WHERE status IN ('EXPIRED', 'ACTIVE') AND plan IN ('premium', 'enterprise', 'basic');
