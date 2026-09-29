WITH scored AS (
    SELECT
        id,
        balance,
        opened_date,
        LEAD(balance) OVER w_ordered AS next_balance,
        LAG(balance) OVER w_ordered AS prev_balance,
        ROW_NUMBER() OVER w_ordered AS row_seq
    FROM accounts
    WINDOW w_ordered AS (PARTITION BY account_type ORDER BY balance, id)
),
scored_accounts AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY next_balance, id) AS event_seq,
        AVG(prev_balance) OVER (ORDER BY prev_balance, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS prev_balance_avg_2_prior,
        FIRST_VALUE(row_seq) IGNORE NULLS OVER (PARTITION BY MONTH(opened_date) ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_row_seq
    FROM scored
),
metrics AS (
    SELECT
        id,
        balance,
        opened_date,
        next_balance,
        row_seq,
        prev_balance_avg_2_prior,
        ROW_NUMBER() OVER (ORDER BY event_seq, id) AS event_seq_2,
        CUME_DIST() OVER (ORDER BY event_seq) AS event_seq_cume_dist,
        FIRST_VALUE(prev_balance_avg_2_prior, TRUE) OVER (ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_prev_balance_avg_2_prior,
        DENSE_RANK() OVER (ORDER BY event_seq DESC) AS event_seq_dense_rank,
        FIRST_VALUE(prev_balance) IGNORE NULLS OVER (ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_prev_balance
    FROM scored_accounts
    WHERE prev_balance >= 34000
),
with_latest AS (
    SELECT
        s.*,
        latest.transaction_txn_time,
        latest.transaction_txn_type
    FROM metrics s
    LEFT JOIN LATERAL (
            SELECT c.txn_time AS transaction_txn_time, c.txn_type AS transaction_txn_type
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
scored_accounts_2 AS (
    SELECT
        id,
        balance,
        opened_date,
        next_balance,
        first_known_prev_balance_avg_2_prior,
        first_known_prev_balance,
        transaction_txn_time,
        NTILE(3) OVER (PARTITION BY transaction_txn_type ORDER BY prev_balance_avg_2_prior, id) AS prev_balance_avg_2_prior_bucket,
        ROW_NUMBER() OVER (PARTITION BY transaction_txn_type ORDER BY next_balance, id) AS seq_no,
        DENSE_RANK() OVER (PARTITION BY transaction_txn_type ORDER BY first_known_prev_balance_avg_2_prior DESC) AS first_known_prev_balance_avg_2_prior_dense_rank,
        NTILE(10) OVER (PARTITION BY DATE_TRUNC('week', transaction_txn_time), transaction_txn_type ORDER BY first_known_prev_balance, id) AS first_known_prev_balance_bucket,
        ROW_NUMBER() OVER (PARTITION BY transaction_txn_type ORDER BY first_known_prev_balance_avg_2_prior, id) AS row_seq_2
    FROM with_latest
),
enriched AS (
    SELECT
        id,
        first_known_prev_balance_avg_2_prior,
        first_known_prev_balance,
        seq_no,
        first_known_prev_balance_bucket,
        MIN(first_known_prev_balance) OVER (ORDER BY next_balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_first_known_prev_balance,
        COUNT(first_known_prev_balance) OVER (ORDER BY next_balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_first_known_prev_balance,
        SUM(first_known_prev_balance) OVER (ORDER BY next_balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_first_known_prev_balance,
        MAX(first_known_prev_balance) OVER (ORDER BY next_balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_first_known_prev_balance
    FROM scored_accounts_2
),
kept AS (
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.merchant IN ('Amazon', 'Grab', 'Shell')
        )
)
SELECT
    id,
    first_known_prev_balance_avg_2_prior,
    first_known_prev_balance,
    seq_no,
    first_known_prev_balance_bucket,
    running_min_first_known_prev_balance,
    running_count_first_known_prev_balance,
    running_sum_first_known_prev_balance,
    running_max_first_known_prev_balance
FROM kept
ORDER BY first_known_prev_balance NULLS FIRST;
