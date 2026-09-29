select a.id, a.customer_id, a.opened_date, a.balance from accounts a where a.balance > 65000 and a.customer_id in (select id from customers where not is_active);
