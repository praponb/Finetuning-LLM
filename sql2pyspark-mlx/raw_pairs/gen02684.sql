select transactions.id, transactions.amount, transactions.account_id, accounts.status from transactions join accounts on accounts.id = transactions.account_id;
