select c.signup_date, c.email, o.store_id from customers c left outer join orders o on o.customer_id = c.id order by c.signup_date desc;
