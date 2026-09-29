WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        LAST_DAY(opened_date) AS opened_date_month_end,
        COALESCE(status, 'N/A') AS status_filled
    FROM accounts
    WHERE status IS NOT NULL
      AND opened_date > DATE '2023-09-18'
),
latest_match AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.account_type,
        s.balance,
        s.opened_date,
        s.status,
        s.status_filled,
        latest.txn_time,
        latest.merchant
    FROM base s
    JOIN LATERAL (
            SELECT c.txn_time, c.merchant
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(balance) OVER (PARTITION BY TO_DATE(txn_time), account_type) AS txn_time_to_date_sum_balance,
        COUNT(balance) OVER (PARTITION BY status ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS balance_count_90d,
        LEAD(balance, 2) OVER w_recent AS next_balance
    FROM latest_match
    WINDOW w_recent AS (PARTITION BY TO_DATE(txn_time), account_type ORDER BY opened_date, id)
),
enriched_accounts AS (
    -- Running and moving aggregates
    SELECT
        id,
        opened_date,
        status,
        txn_time,
        txn_time_to_date_sum_balance,
        next_balance,
        COUNT(next_balance) OVER (PARTITION BY merchant, status ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS next_balance_count_7d,
        MAX(next_balance) OVER (PARTITION BY merchant, account_type ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS next_balance_max_7d
    FROM enriched
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(next_balance_count_7d) OVER (PARTITION BY status ORDER BY next_balance_max_7d, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_sum_next_balance_count_7d,
        AVG(next_balance_count_7d) OVER (PARTITION BY status ORDER BY next_balance_max_7d, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_next_balance_count_7d,
        COUNT(next_balance_count_7d) OVER (PARTITION BY status ORDER BY next_balance_max_7d, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_count_next_balance_count_7d,
        ROW_NUMBER() OVER (PARTITION BY MONTH(opened_date), status ORDER BY next_balance DESC, id) AS event_seq
    FROM enriched_accounts
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('WEEK', txn_time) AS week_start,
        COUNT(DISTINCT status) AS distinct_status,
        PERCENTILE_APPROX(txn_time_to_date_sum_balance, 0.5) AS txn_time_to_date_sum_balance_p50,
        SUM(CASE WHEN status = 'OPEN' THEN last7_avg_next_balance_count_7d ELSE 0 END) AS open_last7_avg_next_balance_count_7d,
        APPROX_PERCENTILE(next_balance_count_7d, 0.5) AS next_balance_count_7d_p50,
        (UNIX_TIMESTAMP(MAX(txn_time)) - UNIX_TIMESTAMP(MIN(txn_time))) / 60 AS span_minutes,
        COUNT(*) FILTER (WHERE status = 'CLOSED') AS closed_rows
    FROM metrics
    GROUP BY DATE_TRUNC('WEEK', txn_time)
)
SELECT
    week_start,
    txn_time_to_date_sum_balance_p50,
    span_minutes,
    closed_rows
FROM summary
ORDER BY week_start
