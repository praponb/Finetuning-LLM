select id, end_date, plan, monthly_fee, rank() over (order by end_date) as rnk, avg(monthly_fee) over (partition by plan) as plan_avg_monthly_fee from subscriptions;
