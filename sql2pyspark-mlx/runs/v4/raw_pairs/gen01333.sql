SELECT id, account_id, amount, txn_time FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY amount DESC, id) AS rn FROM transactions) AS r WHERE rn = 1 ORDER BY amount;
