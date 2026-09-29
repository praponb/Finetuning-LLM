select id, account_id, txn_time, amount, sum(amount) over (partition by account_id order by txn_time, id) as running_amount from transactions where txn_time >= '2025-06-01' order by account_id
