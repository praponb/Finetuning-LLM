WITH latest_match AS (
    SELECT
        s.*,
        latest.txn_type,
        latest.transaction_amount
    FROM accounts s
    JOIN LATERAL (
            SELECT c.txn_type, c.amount AS transaction_amount
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    id,
    customer_id,
    balance
FROM latest_match
ORDER BY id DESC;
