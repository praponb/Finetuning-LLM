SELECT
    id,
    merchant,
    amount,
    RANK() OVER (PARTITION BY merchant ORDER BY amount DESC) AS amount_rank,
    MAX(amount) OVER (PARTITION BY merchant) AS merchant_max_amount
FROM transactions
WHERE txn_type NOT IN ('CREDIT', 'REFUND')
