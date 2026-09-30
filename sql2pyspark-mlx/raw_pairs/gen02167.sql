select a.id, a.status, a.customer_id, a.balance from accounts a where not exists (select 1 from transactions t where t.account_id = a.id and t.is_flagged);
