WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        DATE_SUB(opened_date, 69) opened_date_minus_69d,
        COALESCE(status, 'UNKNOWN') status_clean
    FROM accounts
    WHERE opened_date > TO_DATE('2023-03-13')
),
with_last AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.account_type,
        s.balance,
        latest.txn_type,
        latest.transaction_merchant
    FROM cleaned s
    JOIN LATERAL (
            SELECT c.txn_type, c.merchant AS transaction_merchant
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY balance DESC, id) event_seq,
        COUNT(balance) OVER (PARTITION BY account_type, customer_id) account_type_count_balance,
        AVG(balance) OVER (PARTITION BY account_type, txn_type ORDER BY balance, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) balance_avg_2,
        AVG(balance) OVER (PARTITION BY customer_id, transaction_merchant ORDER BY balance, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) balance_avg_5
    FROM with_last
),
latest_match AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.transaction_txn_type,
        latest.transaction_amount
    FROM scored s
    JOIN LATERAL (
            SELECT c.txn_type AS transaction_txn_type, c.amount AS transaction_amount
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
latest_match_accounts AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.merchant
    FROM latest_match s
    LEFT JOIN LATERAL (
            SELECT c.merchant
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(balance_avg_5) OVER w_running running_sum_balance_avg_5,
        COUNT(balance_avg_5) OVER w_running running_count_balance_avg_5,
        MAX(balance_avg_5) OVER w_running running_max_balance_avg_5,
        ROW_NUMBER() OVER w_part seq_no
    FROM latest_match_accounts
    WHERE account_type_count_balance IS NOT NULL
    WINDOW w_running AS (PARTITION BY txn_type ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_part AS (PARTITION BY transaction_txn_type ORDER BY transaction_amount, id)
)
-- Multi-level aggregation
SELECT
    seq_no,
    COUNT_IF(transaction_merchant = '7-Eleven') AS 7_eleven_events,
    COUNT(*) AS event_count,
    SORT_ARRAY(COLLECT_SET(transaction_merchant)) AS transaction_merchant_list,
    APPROX_PERCENTILE(running_sum_balance_avg_5, ARRAY(0.25, 0.5, 0.75)) AS running_sum_balance_avg_5_quartiles,
    SUM(balance) FILTER (WHERE txn_type = 'DEBIT') AS debit_total_balance,
    MIN(event_seq) AS min_event_seq
FROM enriched
GROUP BY 1
HAVING SUM(balance) FILTER (WHERE txn_type = 'DEBIT') < 1000000
ORDER BY debit_total_balance DESC
