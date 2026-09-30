WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        UPPER(status) AS status_upper,
        COALESCE(status, 'N/A') AS status_filled
    FROM accounts
    WHERE balance > 33000
      AND customer_id IS NOT NULL
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.txn_type IN ('REFUND', 'FEE')
        )
),
with_last AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.transaction_amount
    FROM eligible s
    JOIN LATERAL (
            SELECT c.amount AS transaction_amount
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time <= s.opened_date
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        balance,
        status,
        status_upper,
        transaction_amount,
        ROUND(transaction_amount / SUM(transaction_amount) OVER (PARTITION BY status), 4) AS transaction_amount_share,
        RANK() OVER (PARTITION BY YEAR(opened_date), customer_id ORDER BY balance DESC) AS balance_rank
    FROM with_last
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(transaction_amount_share) OVER w_part AS last4_count_transaction_amount_share,
        MIN(transaction_amount_share) OVER w_part AS last4_min_transaction_amount_share,
        MAX(transaction_amount_share) OVER w_part AS last4_max_transaction_amount_share
    FROM scored
    WINDOW w_part AS (PARTITION BY status ORDER BY balance_rank, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    status,
    status_upper,
    transaction_amount,
    balance_rank,
    last4_min_transaction_amount_share
FROM enriched
ORDER BY last4_min_transaction_amount_share NULLS LAST;
