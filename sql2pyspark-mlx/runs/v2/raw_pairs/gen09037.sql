SELECT *, FLOOR(monthly_fee) AS monthly_fee_whole FROM subscriptions WHERE status = 'EXPIRED';
