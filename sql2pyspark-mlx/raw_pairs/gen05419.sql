select s.id, s.customer_id, c.name, c.email from sessions s inner join customers c on s.customer_id = c.id order by c.name desc
