select s.id, s.status, c.name, c.country from subscriptions s join customers c on s.customer_id = c.id order by country desc, id limit 50;
