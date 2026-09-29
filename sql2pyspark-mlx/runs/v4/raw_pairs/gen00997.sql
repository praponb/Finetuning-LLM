select accounts.id, accounts.opened_date from accounts where accounts.customer_id in (select id from customers where segment in ('SMB', 'Corporate', 'Consumer'));
