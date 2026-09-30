select a.id, a.customer_id from accounts a where exists (select 1 from transactions t where t.account_id = a.id)
