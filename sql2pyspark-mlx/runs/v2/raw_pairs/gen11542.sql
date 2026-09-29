select t.id, t.account_id, t.txn_type, t.txn_time, a.customer_id from transactions t join accounts a on t.account_id = a.id where a.account_type = 'CHECKING';
