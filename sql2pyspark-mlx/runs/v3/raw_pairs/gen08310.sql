select id, status, monthly_fee, max(monthly_fee) over (partition by status) as status_max_monthly_fee from subscriptions where start_date >= '2023-08-01';
