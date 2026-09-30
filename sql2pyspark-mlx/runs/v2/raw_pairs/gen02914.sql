select s.id, s.monthly_fee, c.age, c.city from subscriptions s inner join customers c on s.customer_id = c.id
