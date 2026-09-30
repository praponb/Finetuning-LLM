SELECT
    id,
    merchant,
    account_id,
    txn_time,
    amount,
    LAST_VALUE(amount) OVER (PARTITION BY merchant, account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_amount
FROM transactions
WHERE merchant = 'Amazon';
