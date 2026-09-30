select a.status, count(*) as num_transactions, min(t.txn_time) as first_txn_time from transactions t inner join accounts a on t.account_id = a.id group by a.status;
