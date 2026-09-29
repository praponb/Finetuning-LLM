WITH valid AS (
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        merchant,
        is_flagged,
        CONCAT(merchant, '/', txn_type) merchant_txn_type,
        COALESCE(merchant, 'DIRECT') merchant_filled
    FROM transactions
    WHERE is_flagged
),
enriched AS (
    SELECT
        s.*,
        a.balance,
        a.status
    FROM valid s
    INNER JOIN accounts a
        ON s.account_id = a.id
),
with_dims AS (
    SELECT
        a.id,
        a.account_id,
        a.txn_type,
        a.amount,
        a.balance,
        d.account_type,
        d.opened_date,
        d.status AS account_status
    FROM enriched a
    INNER JOIN accounts d
        ON a.account_id = d.id
    WHERE YEAR(d.opened_date) = 2024
),
latest AS (
    SELECT
        id,
        txn_type,
        amount,
        balance,
        account_status
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY amount DESC, id ASC) AS rank_in_group
        FROM with_dims
    ) t
    WHERE rank_in_group = 1
),
latest_transactions AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY account_status, txn_type ORDER BY amount DESC NULLS FIRST, id ASC) AS latest_rank
    FROM latest
    QUALIFY latest_rank = 1
)
SELECT
    id,
    amount,
    balance,
    account_status,
    CASE WHEN account_status RLIKE '^(OPEN|FROZEN)$' THEN 'GROUP_B' WHEN account_status RLIKE '^(CLOSED)$' THEN 'GROUP_A' ELSE 'PRIORITY' END AS account_status_zone
FROM latest_transactions
ORDER BY balance ASC, account_status ASC
