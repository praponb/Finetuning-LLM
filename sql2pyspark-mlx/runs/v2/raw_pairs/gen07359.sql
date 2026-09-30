select a.id, a.status from accounts a where not exists (select 1 from transactions t where t.account_id = a.id) order by a.status asc, id asc limit 5;
