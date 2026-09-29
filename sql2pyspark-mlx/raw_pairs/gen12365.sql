select o.id, o.amount, c.name, c.segment from orders o join customers c on o.customer_id = c.id;
