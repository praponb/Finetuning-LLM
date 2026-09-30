WITH with_last AS (
    SELECT
        s.id,
        s.account_type,
        x.transaction_txn_type
    FROM accounts s
    JOIN LATERAL (
            SELECT c.txn_type AS transaction_txn_type
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    id,
    account_type,
    transaction_txn_type
FROM with_last
ORDER BY transaction_txn_type DESC NULLS FIRST
