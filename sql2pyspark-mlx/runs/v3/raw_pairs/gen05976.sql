select a.id, a.status, a.customer_id, a.balance from accounts a where a.account_type = 'SAVINGS' and not exists (select 1 from transactions t where t.account_id = a.id);
