select s.id, s.device, c.name from sessions s inner join customers c on s.customer_id = c.id;
