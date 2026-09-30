select *, date_trunc('MONTH', start_date) as start_date_period from subscriptions where monthly_fee > 210 and end_date between '2024-01-23' and '2025-03-01' and monthly_fee >= 120;
