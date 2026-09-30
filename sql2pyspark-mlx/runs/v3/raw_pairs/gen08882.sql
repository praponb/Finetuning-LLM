select a.id, a.status, a.customer_id, a.balance from accounts a where a.customer_id in (select id from customers where not is_active) order by 2;
