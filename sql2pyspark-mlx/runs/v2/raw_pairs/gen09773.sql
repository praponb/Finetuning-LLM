SELECT s.id, s.plan, s.monthly_fee, c.name FROM subscriptions s LEFT OUTER JOIN customers c ON s.customer_id = c.id
