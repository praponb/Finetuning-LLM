select s.start_date, c.name, c.age from subscriptions s right outer join customers c on s.customer_id = c.id where c.email is not null and c.email not like '%@gmail.com' order by start_date asc
