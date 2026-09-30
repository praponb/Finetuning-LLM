SELECT account_id, txn_time, amount,
       SUM(amount) OVER (PARTITION BY account_id ORDER BY txn_time, id) AS running_total
FROM transactions;
