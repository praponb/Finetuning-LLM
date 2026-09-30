SELECT a.id, a.opened_date, t.last_txn_time FROM accounts a LEFT OUTER JOIN (SELECT account_id, MAX(txn_time) AS last_txn_time FROM transactions GROUP BY account_id) t ON a.id = t.account_id;
