select customer_id, status, min(start_date) as min_start_date from subscriptions where status = 'EXPIRED' and status = 'CANCELLED' and monthly_fee is not null group by customer_id, status
