select plan, customer_id, avg(monthly_fee) as avg_monthly_fee, max(start_date) as latest_start_date from subscriptions where plan not in ('standard', 'basic') group by plan, customer_id;
