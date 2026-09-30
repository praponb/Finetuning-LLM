select c.id, c.age, c.segment, c.signup_date from customers c where lower(c.name) like '%ia %' and not exists (select 1 from orders o where o.customer_id = c.id);
