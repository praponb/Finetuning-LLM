select c.segment, c.is_active, s.device, s.customer_id from customers c left join sessions s on s.customer_id = c.id
