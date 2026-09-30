select c.country, s.plan from customers c left join subscriptions s on s.customer_id = c.id
