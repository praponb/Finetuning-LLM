select a.id, a.status, a.opened_date, a.customer_id from accounts a where not exists (select 1 from transactions t where t.account_id = a.id) order by a.opened_date, a.customer_id desc, id limit 5;
