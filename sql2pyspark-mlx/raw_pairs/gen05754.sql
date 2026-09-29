select plan, status, avg(monthly_fee) avg_monthly_fee from subscriptions group by 1, 2 having avg(monthly_fee) < 90;
