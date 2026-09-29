select id, end_date, plan, customer_id from subscriptions where month(start_date) in (1, 2, 11) and plan = 'basic'
