select c.signup_date, c.city, o.shipping_fee, o.amount, o.discount from customers c inner join orders o on c.id = o.customer_id where c.segment is not null order by c.city;
