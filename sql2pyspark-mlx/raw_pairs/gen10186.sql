select c.id, c.credit_limit from customers c where c.is_active and not exists (select 1 from subscriptions s where s.customer_id = c.id and s.customer_id is null) order by id
