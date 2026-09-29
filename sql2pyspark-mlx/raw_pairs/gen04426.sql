WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        balance,
        opened_date,
        status,
        COALESCE(status, 'Unknown') AS status_filled
    FROM accounts
    WHERE opened_date > DATE '2023-10-07'
),
latest_match AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.txn_type,
        latest.amount
    FROM valid s
    LEFT JOIN LATERAL (
            SELECT c.txn_type, c.amount
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        balance,
        status,
        status_filled,
        txn_type,
        SUM(balance) OVER (PARTITION BY customer_id, txn_type ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance,
        DENSE_RANK() OVER (PARTITION BY customer_id, status ORDER BY balance DESC NULLS FIRST) AS balance_dense_rank,
        AVG(amount) OVER (PARTITION BY txn_type) AS txn_type_avg_amount,
        ROW_NUMBER() OVER (PARTITION BY YEAR(opened_date), txn_type ORDER BY opened_date DESC, id) AS row_seq,
        NTILE(10) OVER (PARTITION BY customer_id ORDER BY balance, id) AS balance_bucket
    FROM latest_match
),
latest_match_accounts AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.status,
        s.status_filled,
        s.txn_type,
        s.running_balance,
        s.balance_bucket,
        top1.txn_time
    FROM metrics s
    LEFT JOIN LATERAL (
            SELECT c.txn_time
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_dims AS (
    -- Attach customers attributes
    SELECT a.*, d.segment
    FROM latest_match_accounts a
    LEFT JOIN customers d
        ON a.customer_id <=> d.id
    WHERE d.is_active
)
-- Join a dimension and aggregate
SELECT
    d.city,
    d.country,
    COUNT(*) AS rows_total,
    SUM(a.running_balance) AS total_running_balance,
    GROUPING_ID() AS level_id
FROM with_dims a
INNER JOIN customers d ON a.customer_id = d.id
GROUP BY GROUPING SETS ((d.city, d.country), (d.city))
ORDER BY rows_total;
