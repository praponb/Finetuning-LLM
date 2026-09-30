select t.id, t.amount, t.txn_type, a.account_type, a.balance from transactions t inner join accounts a on t.account_id = a.id;
