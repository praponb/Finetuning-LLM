select s.id, s.customer_id, s.plan from subscriptions s where s.customer_id in (select id from customers where credit_limit between 13400 and 15000);
