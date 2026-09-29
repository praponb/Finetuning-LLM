select c.credit_limit, c.email, s.status, s.customer_id from customers c left outer join subscriptions s on s.customer_id = c.id where s.customer_id is not null;
