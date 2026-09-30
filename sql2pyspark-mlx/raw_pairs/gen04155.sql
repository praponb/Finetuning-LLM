WITH kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM accounts s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.txn_time <= s.opened_date
        )
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (ORDER BY balance DESC NULLS LAST) AS balance_rank,
        ROW_NUMBER() OVER w_part AS seq_no,
        NTILE(5) OVER (PARTITION BY status ORDER BY balance DESC, id) AS balance_bucket,
        COUNT(balance) OVER (PARTITION BY customer_id) AS customer_id_count_balance,
        NTILE(5) OVER (ORDER BY balance, id) AS balance_bucket_2
    FROM kept
    WINDOW w_part AS (PARTITION BY account_type ORDER BY opened_date DESC, id)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        account_type,
        balance,
        opened_date,
        balance_rank,
        balance_bucket_2,
        NTILE(5) OVER (PARTITION BY account_type ORDER BY seq_no, id) AS seq_no_bucket,
        NTILE(3) OVER (PARTITION BY MONTH(opened_date), status ORDER BY balance_bucket, id) AS balance_bucket_bucket,
        AVG(balance_bucket) OVER (PARTITION BY MONTH(opened_date), customer_id ORDER BY balance_rank, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS balance_bucket_avg_6_prior,
        NTILE(10) OVER (ORDER BY seq_no, id) AS seq_no_bucket_2,
        ROW_NUMBER() OVER w_recent AS row_seq
    FROM scored
    WHERE customer_id IS NULL
    WINDOW w_recent AS (PARTITION BY status ORDER BY balance DESC, id)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(row_seq) OVER w_hist AS prev_row_seq,
        LEAD(row_seq) OVER w_hist AS next_row_seq,
        LAG(balance) OVER w_hist AS prev_balance,
        ROW_NUMBER() OVER w_hist AS seq_no
    FROM enriched
    WINDOW w_hist AS (PARTITION BY account_type ORDER BY balance_bucket_avg_6_prior, id)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        account_type,
        opened_date,
        balance_rank,
        balance_bucket_2,
        seq_no_bucket_2,
        prev_row_seq,
        next_row_seq,
        prev_balance,
        seq_no,
        NTILE(10) OVER (PARTITION BY MONTH(opened_date) ORDER BY balance_rank, id) AS balance_rank_bucket,
        LAG(prev_balance, 1) OVER (PARTITION BY account_type ORDER BY balance_bucket_2, id) AS prev_prev_balance,
        AVG(balance_bucket_bucket) OVER (PARTITION BY account_type) AS account_type_avg_balance_bucket_bucket
    FROM with_windows
),
with_windows_accounts AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(prev_row_seq) OVER (PARTITION BY account_type ORDER BY seq_no_bucket_2, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS prev_row_seq_avg_3,
        PERCENT_RANK() OVER (PARTITION BY account_type ORDER BY prev_balance DESC) AS prev_balance_percent_rank,
        AVG(balance_rank_bucket) OVER (PARTITION BY account_type ORDER BY seq_no_bucket_2, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS balance_rank_bucket_avg_6,
        ROUND(prev_row_seq / SUM(prev_row_seq) OVER (PARTITION BY account_type), 4) AS prev_row_seq_share,
        DENSE_RANK() OVER (PARTITION BY account_type ORDER BY prev_prev_balance) AS prev_prev_balance_dense_rank
    FROM metrics
),
scored_accounts AS (
    -- Ranking and gap features
    SELECT
        id,
        opened_date,
        seq_no_bucket_2,
        prev_row_seq,
        prev_balance,
        seq_no,
        prev_prev_balance,
        account_type_avg_balance_bucket_bucket,
        prev_balance_percent_rank,
        prev_prev_balance_dense_rank,
        LEAD(account_type_avg_balance_bucket_bucket, 1) OVER (ORDER BY prev_balance, id) AS next_account_type_avg_balance_bucket_bucket,
        STDDEV_SAMP(balance_bucket_2) OVER (PARTITION BY DATE_TRUNC('YEAR', opened_date), account_type) AS opened_date_period_stddev_samp_balance_bucket_2
    FROM with_windows_accounts
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        prev_prev_balance,
        prev_balance
    FROM scored_accounts
) src
UNPIVOT (
    value FOR metric IN (prev_prev_balance, prev_balance)
) long_rows;
