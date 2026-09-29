WITH scored AS (
    SELECT
        *,
        SUM(balance) OVER (PARTITION BY account_type) AS account_type_sum_balance,
        NTILE(4) OVER w_hist AS balance_bucket
    FROM accounts
    WINDOW w_hist AS (ORDER BY balance DESC, id)
),
joined AS (
    SELECT
        a.id,
        a.customer_id,
        a.account_type,
        a.opened_date,
        p.signup_date,
        p.email,
        p.credit_limit
    FROM scored a
    LEFT JOIN customers p
        ON a.customer_id = p.id
),
with_windows AS (
    SELECT
        id,
        customer_id,
        opened_date,
        MIN(credit_limit) OVER (PARTITION BY account_type ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_credit_limit,
        AVG(credit_limit) OVER (PARTITION BY account_type ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_credit_limit,
        MAX(credit_limit) OVER (PARTITION BY account_type ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_credit_limit
    FROM joined
),
enriched AS (
    SELECT
        id,
        opened_date,
        running_min_credit_limit,
        running_avg_credit_limit,
        running_max_credit_limit,
        AVG(running_avg_credit_limit) OVER (PARTITION BY customer_id ORDER BY opened_date, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS running_avg_credit_limit_avg_5,
        SUM(running_min_credit_limit) OVER (PARTITION BY MONTH(opened_date), customer_id ORDER BY running_min_credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_running_min_credit_limit,
        SUM(running_avg_credit_limit) OVER (PARTITION BY customer_id ORDER BY running_avg_credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_running_avg_credit_limit
    FROM with_windows
    WHERE customer_id IN (11, 16, 36)
),
latest_match AS (
    SELECT
        s.id,
        s.running_min_credit_limit,
        s.running_max_credit_limit,
        s.cumulative_running_min_credit_limit,
        top1.txn_time,
        top1.txn_type
    FROM enriched s
    JOIN LATERAL (
            SELECT c.txn_time, c.txn_type
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_gaps AS (
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM latest_match
),
sessionized AS (
    SELECT
        id,
        running_max_credit_limit,
        txn_time,
        txn_type,
        SUM(session_start_flag) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
with_latest AS (
    SELECT
        s.*,
        x.transaction_txn_type,
        x.merchant
    FROM sessionized s
    LEFT JOIN LATERAL (
            SELECT c.txn_type AS transaction_txn_type, c.merchant
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    id,
    running_max_credit_limit,
    txn_time,
    txn_type,
    session_id,
    transaction_txn_type,
    CASE WHEN txn_type RLIKE '^(FEE|CREDIT)$' THEN 'CORE' ELSE 'GROUP_B' END AS txn_type_zone
FROM with_latest
ORDER BY transaction_txn_type DESC NULLS LAST;
