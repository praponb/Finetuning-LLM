select a.id, a.status from accounts a where not exists (select 1 from transactions t where t.account_id = a.id) order by status asc, id asc limit 10
