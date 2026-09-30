select a.id, a.account_type, a.opened_date, a.status from accounts a where not exists (select 1 from transactions t where t.account_id = a.id and t.account_id in (27, 39)) order by a.id desc limit 3;
