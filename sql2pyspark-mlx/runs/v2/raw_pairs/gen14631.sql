SELECT
    transactions.txn_time,
    transactions.merchant,
    accounts.opened_date,
    accounts.status
FROM transactions
LEFT OUTER JOIN accounts ON transactions.account_id = accounts.id
ORDER BY merchant;
