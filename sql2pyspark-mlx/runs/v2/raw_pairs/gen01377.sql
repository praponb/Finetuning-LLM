select o.id, o.discount, o.amount, c.city, c.is_active, c.name from orders o join customers c on c.id = o.customer_id where c.country not in ('TH', 'JP') order by id desc limit 10;
