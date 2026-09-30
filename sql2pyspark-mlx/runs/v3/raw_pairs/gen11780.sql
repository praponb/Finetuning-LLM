SELECT transactions.amount, accounts.balance, accounts.account_type FROM transactions LEFT JOIN accounts ON transactions.account_id = accounts.id WHERE transactions.amount >= 3900;
