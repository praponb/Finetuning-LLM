select accounts.status, accounts.balance, transactions.amount from accounts left outer join transactions on transactions.account_id = accounts.id where transactions.account_id in (9, 13)
