SELECT
    id,
    txn_time,
    account_id,
    amount,
    PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('YEAR', txn_time), account_id ORDER BY amount DESC) AS amount_pct_rank,
    LEAD(amount, 1) OVER (PARTITION BY DATE_TRUNC('WEEK', txn_time) ORDER BY txn_time, id) AS next_amount
FROM transactions;
