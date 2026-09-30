SELECT id, plan, FLOOR(monthly_fee) AS monthly_fee_whole FROM subscriptions WHERE monthly_fee <= 100 AND (start_date < '2023-11-01' OR monthly_fee >= 80);
