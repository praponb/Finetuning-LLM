WITH attributed AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM accounts s
    INNER JOIN subscriptions sub
        ON s.customer_id = sub.customer_id
       AND s.opened_date BETWEEN sub.start_date AND sub.end_date
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        account_type,
        opened_date,
        subscription_id,
        MIN(balance) OVER w_hist AS running_min_balance,
        MAX(balance) OVER w_hist AS running_max_balance,
        COUNT(balance) OVER w_hist AS running_count_balance
    FROM attributed
    WHERE plan IN ('basic', 'standard', 'enterprise')
    WINDOW w_hist AS (PARTITION BY account_type ORDER BY opened_date, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.account_type,
        e.opened_date,
        e.subscription_id,
        e.running_min_balance,
        e.running_max_balance,
        e.running_count_balance,
        b.age,
        b.signup_date
    FROM scored e
    LEFT JOIN customers b
        ON e.customer_id = b.id
),
with_last AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.transaction_txn_type,
        top1.transaction_txn_time
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.txn_type AS transaction_txn_type, c.txn_time AS transaction_txn_time
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_latest AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.opened_date,
        s.subscription_id,
        s.running_max_balance,
        s.signup_date,
        s.transaction_txn_time,
        top1.transaction_txn_time_2
    FROM with_last s
    LEFT JOIN LATERAL (
            SELECT c.txn_time AS transaction_txn_time_2
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        src.id,
        src.customer_id,
        src.opened_date,
        src.subscription_id,
        src.running_max_balance,
        src.signup_date,
        src.transaction_txn_time_2,
        c.name
    FROM with_latest src
    INNER JOIN customers c
        ON src.customer_id = c.id
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN transaction_txn_time_2 - LAG(transaction_txn_time_2) OVER (PARTITION BY customer_id ORDER BY transaction_txn_time_2, id, subscription_id) > INTERVAL 90 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM with_dims
),
sessions_accounts AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY customer_id ORDER BY transaction_txn_time_2, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
)
SELECT
    id,
    customer_id,
    subscription_id,
    running_max_balance,
    transaction_txn_time_2,
    name,
    new_group_flag,
    session_id
FROM sessions_accounts
ORDER BY session_id DESC
