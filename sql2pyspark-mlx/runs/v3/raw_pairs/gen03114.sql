select plan, customer_id from subscriptions where plan = 'basic' intersect select plan, customer_id from subscriptions where end_date <= '2023-03-16';
