select c.id, c.country, c.name, c.segment from customers c where c.id in (select customer_id from orders) order by name desc, country asc
