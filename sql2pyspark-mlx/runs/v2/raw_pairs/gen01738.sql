select c.id, c.segment from customers c where exists (select 1 from subscriptions s where s.customer_id = c.id and s.monthly_fee > 130);
