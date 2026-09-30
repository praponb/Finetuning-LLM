WITH eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM accounts s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.txn_time >= s.opened_date
        )
)
SELECT
    id,
    customer_id,
    account_type,
    balance,
    opened_date
FROM eligible
ORDER BY account_type DESC
