SELECT t.id, t.merchant, t.txn_time, a.opened_date FROM transactions t INNER JOIN accounts a ON t.account_id = a.id WHERE a.balance BETWEEN 24000 AND 68000;
