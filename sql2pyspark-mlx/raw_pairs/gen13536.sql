select a.id, a.opened_date, a.status, a.customer_id from accounts a where a.customer_id not in (select id from customers where city = 'Chiang Mai');
