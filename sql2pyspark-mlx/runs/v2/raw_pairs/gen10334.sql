select subscriptions.id, subscriptions.monthly_fee, customers.age, customers.credit_limit, customers.country from subscriptions join customers on subscriptions.customer_id = customers.id
