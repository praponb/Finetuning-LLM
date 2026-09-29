select s.id, s.status, s.plan from subscriptions s where s.customer_id not in (select id from customers where signup_date between '2023-10-01' and '2023-11-01');
