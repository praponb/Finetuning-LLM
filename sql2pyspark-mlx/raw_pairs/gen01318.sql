SELECT
    id,
    amount,
    is_flagged
FROM transactions
QUALIFY ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY txn_time DESC NULLS LAST, id) = 1;
