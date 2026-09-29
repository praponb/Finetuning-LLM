select id, account_id, txn_time, rank() over (partition by account_id order by txn_time) as rnk from transactions
