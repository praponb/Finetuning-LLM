select accounts.id, accounts.balance from accounts where not exists (select 1 from transactions where transactions.account_id = accounts.id and transactions.txn_time >= '2023-09-12');
