select s.customer_id, c.name, c.is_active, c.email from sessions s right outer join customers c on s.customer_id = c.id order by customer_id desc;
