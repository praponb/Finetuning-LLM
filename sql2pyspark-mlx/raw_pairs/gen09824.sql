WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        account_type,
        opened_date,
        status,
        CASE WHEN status IN ('OPEN', 'FROZEN') THEN 'Priority' ELSE 'Other' END AS status_group,
        UPPER(status) AS status_upper
    FROM accounts
    WHERE opened_date >= DATE '2023-07-18'
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        opened_date,
        status_group,
        status_upper
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status, account_type ORDER BY opened_date DESC, id) = 1
),
with_last AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.transaction_txn_time,
        top1.amount
    FROM latest s
    LEFT JOIN LATERAL (
            SELECT c.txn_time AS transaction_txn_time, c.amount
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time <= s.opened_date
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        status_group,
        status_upper,
        transaction_txn_time,
        LEAD(amount) OVER (ORDER BY amount, id) AS next_amount,
        ROW_NUMBER() OVER (ORDER BY amount, id) AS row_seq
    FROM with_last
    WHERE amount BETWEEN 2400 AND 2500
)
SELECT
    id,
    status_group,
    transaction_txn_time,
    next_amount,
    row_seq
FROM scored;
