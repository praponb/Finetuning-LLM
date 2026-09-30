select a.id, a.balance from accounts a where a.opened_date <= '2024-11-01' and not exists (select 1 from transactions t where t.account_id = a.id)
