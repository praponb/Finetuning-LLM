select a.id, a.status from accounts a where a.opened_date >= '2024-06-01' and exists (select 1 from transactions t where t.account_id = a.id) order by status;
