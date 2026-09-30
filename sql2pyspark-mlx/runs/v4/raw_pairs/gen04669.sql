select s.id, s.customer_id, s.end_date, s.plan from subscriptions s where s.customer_id not in (select id from customers where is_active = false) order by customer_id desc, id limit 5;
