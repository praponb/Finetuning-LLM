select a.id, a.customer_id, c.city, c.is_active, c.segment from accounts a left join customers c on c.id = a.customer_id
