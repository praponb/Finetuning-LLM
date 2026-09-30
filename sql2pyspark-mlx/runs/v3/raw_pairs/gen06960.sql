select round(stddev(monthly_fee), 2) as stddev_monthly_fee, avg(monthly_fee) as avg_monthly_fee from subscriptions;
