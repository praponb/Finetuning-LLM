select id, start_date, monthly_fee, plan, last_day(end_date) as end_date_month_end from subscriptions where monthly_fee between 130 and 160 and plan in ('premium', 'basic') and customer_id is null;
