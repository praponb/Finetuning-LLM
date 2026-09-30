select s.id, s.customer_id, s.status, s.plan from subscriptions s where s.plan = 'basic' and s.customer_id in (select id from customers where age = 45)
