select accounts.id, accounts.account_type from accounts where exists (select 1 from transactions where transactions.account_id = accounts.id);
