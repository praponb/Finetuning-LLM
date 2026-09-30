select plan, quarter(end_date) as end_quarter, round(avg(monthly_fee), 2) as avg_monthly_fee from subscriptions group by 1, 2 order by avg_monthly_fee desc, plan
