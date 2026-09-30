select a.id, a.account_type, a.status from accounts a where a.id in (select account_id from transactions);
