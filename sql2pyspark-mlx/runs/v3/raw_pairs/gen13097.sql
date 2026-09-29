SELECT id, status, monthly_fee, FLOOR(monthly_fee) monthly_fee_whole FROM subscriptions WHERE plan NOT IN ('premium', 'standard') OR end_date BETWEEN '2023-08-21' AND '2024-05-01'
