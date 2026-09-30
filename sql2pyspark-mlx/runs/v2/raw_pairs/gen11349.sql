select a.id, a.balance from accounts a where a.id in (select account_id from transactions);
