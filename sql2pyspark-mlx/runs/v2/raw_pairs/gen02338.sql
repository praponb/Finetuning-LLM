SELECT id, monthly_fee, SUBSTRING(status, 1, 4) status_prefix FROM subscriptions WHERE end_date >= '2023-12-01' OR monthly_fee >= 240;
