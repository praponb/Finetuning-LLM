select s.carrier, o.store_id, c.signup_date, c.email from shipments s inner join orders o on s.order_id = o.id full outer join customers c on o.customer_id = c.id where year(c.signup_date) = 2025
