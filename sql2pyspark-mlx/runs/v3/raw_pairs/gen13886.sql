SELECT
    id,
    txn_time,
    merchant,
    account_id
FROM transactions
ORDER BY account_id DESC, merchant;
