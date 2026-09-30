WITH metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        account_type,
        balance,
        opened_date,
        status,
        AVG(balance) OVER (PARTITION BY account_type ORDER BY opened_date, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS balance_avg_5,
        LEAD(balance, 2) OVER w_running AS next_balance,
        NTILE(5) OVER (PARTITION BY customer_id ORDER BY balance, id) AS balance_bucket,
        AVG(balance) OVER (ORDER BY balance, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS balance_avg_2_prior
    FROM accounts
    WINDOW w_running AS (PARTITION BY customer_id ORDER BY opened_date, id)
),
with_last AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.account_type,
        s.balance,
        s.status,
        s.balance_avg_5,
        s.next_balance,
        s.balance_bucket,
        x.merchant,
        x.txn_type
    FROM metrics s
    LEFT JOIN LATERAL (
            SELECT c.merchant, c.txn_type
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time <= s.opened_date
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(next_balance) OVER (PARTITION BY merchant, status ORDER BY balance_bucket, id) AS next_next_balance,
        LAG(balance) OVER (PARTITION BY merchant, status ORDER BY balance_bucket, id) AS prev_balance,
        LEAD(balance) OVER (PARTITION BY merchant, status ORDER BY balance_bucket, id) AS next_balance_2,
        ROW_NUMBER() OVER (PARTITION BY merchant, status ORDER BY balance_bucket, id) AS row_seq,
        LAST_VALUE(next_balance, TRUE) OVER (PARTITION BY status ORDER BY balance_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_next_balance
    FROM with_last
    WHERE balance <= 33000
),
with_last_accounts AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.balance,
        s.txn_type,
        top1.transaction_amount
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.amount AS transaction_amount
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY balance DESC NULLS FIRST, id) AS row_num
        FROM with_last_accounts
    ) x
    WHERE row_num = 1
)
SELECT
    id,
    balance,
    transaction_amount
FROM latest
ORDER BY id, transaction_amount DESC
LIMIT 10
