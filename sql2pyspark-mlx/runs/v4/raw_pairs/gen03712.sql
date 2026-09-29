select a.id, a.account_type, a.status, a.balance from accounts a where a.customer_id in (select id from customers where name like 'Oli%');
