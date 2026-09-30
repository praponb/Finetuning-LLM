select c.email, s.start_date, s.status, s.end_date from customers c left outer join subscriptions s on s.customer_id = c.id order by status asc;
