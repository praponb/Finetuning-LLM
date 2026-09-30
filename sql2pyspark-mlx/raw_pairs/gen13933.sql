WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, CAST(balance AS INT) AS balance_int
    FROM accounts
    WHERE opened_date > TO_DATE('2023-08-16')
      AND customer_id IS NOT NULL
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        customer_id,
        balance,
        opened_date,
        balance_int
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY balance NULLS FIRST, id) = 1
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY balance NULLS LAST, id) AS latest_rank
    FROM ranked
    QUALIFY latest_rank = 1
),
latest_match AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.balance,
        s.opened_date,
        s.balance_int,
        top1.amount,
        top1.transaction_merchant
    FROM deduped s
    JOIN LATERAL (
            SELECT c.amount, c.merchant AS transaction_merchant
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
deduped_accounts AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        balance,
        amount,
        transaction_merchant
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY transaction_merchant ORDER BY amount DESC NULLS FIRST, id) AS rank_in_group
        FROM latest_match
    ) ranked
    WHERE rank_in_group = 1
),
with_latest AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.amount,
        s.transaction_merchant,
        x.transaction_amount,
        x.txn_type
    FROM deduped_accounts s
    LEFT JOIN LATERAL (
            SELECT c.amount AS transaction_amount, c.txn_type
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    id,
    amount,
    transaction_merchant,
    transaction_amount,
    txn_type
FROM with_latest
ORDER BY txn_type;
