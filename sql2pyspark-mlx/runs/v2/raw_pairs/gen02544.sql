select s.customer_id, s.device, c.country from sessions s left join customers c on s.customer_id = c.id where c.is_active order by c.country desc
