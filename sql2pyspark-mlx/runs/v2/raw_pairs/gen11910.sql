select a.id, a.status, a.balance, a.opened_date from accounts a where exists (select 1 from transactions t where t.account_id = a.id);
