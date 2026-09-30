select a.id, a.customer_id, a.account_type, a.balance from accounts a where a.customer_id in (select id from customers where city in ('Bangkok', 'Chiang Mai'));
