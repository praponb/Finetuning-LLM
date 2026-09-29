select a.id, a.account_type from accounts a where a.customer_id in (select id from customers where not is_active) order by a.account_type
