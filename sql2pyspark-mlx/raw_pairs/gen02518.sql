SELECT id, plan, status, CAST(monthly_fee AS INT) AS monthly_fee_int FROM subscriptions WHERE plan = 'enterprise' AND (monthly_fee > 120 OR customer_id IS NULL) ORDER BY plan, id DESC;
