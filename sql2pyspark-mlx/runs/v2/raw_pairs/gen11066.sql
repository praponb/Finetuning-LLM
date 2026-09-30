select id, customer_id, monthly_fee, sum(monthly_fee) over (partition by customer_id) as customer_id_sum_monthly_fee from subscriptions where plan = 'standard';
