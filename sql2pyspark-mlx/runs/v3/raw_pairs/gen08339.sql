SELECT c.age, c.city, c.email, s.monthly_fee, s.status FROM customers c LEFT OUTER JOIN subscriptions s ON s.customer_id = c.id
