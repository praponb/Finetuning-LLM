select accounts.id, accounts.account_type, accounts.status, accounts.balance from accounts where accounts.id in (select account_id from transactions where account_id = 7);
