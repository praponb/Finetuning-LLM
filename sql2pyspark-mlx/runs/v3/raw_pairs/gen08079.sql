select a.id, a.customer_id from accounts a where a.balance <= 39000 and not exists (select 1 from transactions t where t.account_id = a.id and t.is_flagged = false) order by a.id desc
