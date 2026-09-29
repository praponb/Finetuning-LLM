select id, plan, status, end_date, monthly_fee, lag(monthly_fee, 1, 0) over (partition by plan, status order by end_date, id) as prev_monthly_fee from subscriptions;
