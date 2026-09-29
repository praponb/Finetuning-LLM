WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        FLOOR(balance) AS balance_whole,
        LENGTH(account_type) AS account_type_length
    FROM accounts
    WHERE customer_id IS NOT NULL
),
latest_match AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.balance,
        s.opened_date,
        s.status,
        s.balance_whole,
        s.account_type_length,
        x.txn_time,
        x.txn_type
    FROM base s
    JOIN LATERAL (
            SELECT c.txn_time, c.txn_type
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time <= s.opened_date
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id, customer_id
    FROM latest_match
    INTERSECT
    SELECT id, customer_id
    FROM accounts
    WHERE status IN ('CLOSED', 'FROZEN')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest_match s
    JOIN kept_keys k ON s.id = k.id AND s.customer_id = k.customer_id
),
scored AS (
    -- Ranking and gap features
    SELECT
        opened_date,
        status,
        balance_whole,
        txn_time,
        SUM(balance_whole) OVER w_ordered AS txn_type_sum_balance_whole,
        SUM(balance) OVER (PARTITION BY customer_id) AS customer_id_sum_balance
    FROM reconciled
    WHERE HOUR(txn_time) BETWEEN 6 AND 12
    WINDOW w_ordered AS (PARTITION BY txn_type)
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(txn_type_sum_balance_whole) OVER (PARTITION BY MONTH(opened_date), status ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS txn_type_sum_balance_whole_sum_30d,
        AVG(customer_id_sum_balance) OVER w_ordered AS opened_date_period_avg_customer_id_sum_balance
    FROM scored
    WINDOW w_ordered AS (PARTITION BY DATE_TRUNC('WEEK', opened_date))
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(balance_whole) OVER (PARTITION BY YEAR(opened_date), status ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS balance_whole_count_30d,
        AVG(txn_type_sum_balance_whole_sum_30d) OVER (PARTITION BY status ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS txn_type_sum_balance_whole_sum_30d_avg_90d,
        AVG(customer_id_sum_balance) OVER (PARTITION BY status ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS customer_id_sum_balance_avg_30d
    FROM enriched
),
kept_keys_2 AS (
    -- Set difference on keys
    SELECT status
    FROM metrics
    EXCEPT DISTINCT
    SELECT status
    FROM accounts
    WHERE account_type = 'LOAN'
),
reconciled_accounts AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    JOIN kept_keys_2 k ON s.status = k.status
)
SELECT
    balance_whole,
    txn_time,
    customer_id_sum_balance,
    txn_type_sum_balance_whole_sum_30d,
    opened_date_period_avg_customer_id_sum_balance,
    txn_type_sum_balance_whole_sum_30d_avg_90d
FROM reconciled_accounts
ORDER BY opened_date_period_avg_customer_id_sum_balance DESC NULLS LAST, txn_type_sum_balance_whole_sum_30d_avg_90d
