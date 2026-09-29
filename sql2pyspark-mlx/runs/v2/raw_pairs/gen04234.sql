select s.id, s.status, s.start_date, c.credit_limit, c.is_active from subscriptions s right join customers c on s.customer_id = c.id
