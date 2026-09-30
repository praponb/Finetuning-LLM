SELECT a.account_type, a.balance, t.txn_time, t.merchant FROM accounts a LEFT JOIN transactions t ON a.id = t.account_id WHERE t.txn_type <> 'DEBIT' ORDER BY merchant;
