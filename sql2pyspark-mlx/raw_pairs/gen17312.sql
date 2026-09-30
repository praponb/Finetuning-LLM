select a.id, a.customer_id, a.status from accounts a where a.customer_id in (select id from customers where is_active = true) order by a.id desc
