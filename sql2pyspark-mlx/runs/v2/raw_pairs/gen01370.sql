select a.id, a.account_type, a.balance from accounts a where a.balance = (select max(b.balance) from accounts b where b.account_type = a.account_type)
