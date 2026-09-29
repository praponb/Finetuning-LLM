WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT(account_type, ' ', status) AS account_type_status,
        CONCAT(account_type, ' ', status) AS account_type_status_2,
        COALESCE(status, 'DIRECT') AS status_filled
    FROM accounts
    WHERE status IS NOT NULL
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        account_type,
        balance,
        opened_date,
        status,
        account_type_status_2,
        status_filled,
        COUNT(balance) OVER (PARTITION BY account_type) AS account_type_count_balance,
        PERCENT_RANK() OVER (PARTITION BY MONTH(opened_date), account_type ORDER BY balance DESC NULLS LAST) AS balance_percent_rank,
        MIN(balance) OVER (PARTITION BY status) AS status_min_balance
    FROM cleaned
    WHERE balance <= 53000
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        balance,
        opened_date,
        account_type_count_balance,
        balance_percent_rank,
        ROW_NUMBER() OVER (ORDER BY balance DESC, id) AS row_seq,
        AVG(status_min_balance) OVER w_part AS status_min_balance_avg_3_prior,
        AVG(status_min_balance) OVER w_hist AS status_min_balance_avg_3
    FROM enriched
    WINDOW w_part AS (PARTITION BY MONTH(opened_date) ORDER BY balance, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), w_hist AS (ORDER BY status_min_balance, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(status_min_balance_avg_3_prior, 1, 0) OVER (ORDER BY account_type_count_balance, id) AS prev_status_min_balance_avg_3_prior,
        NTILE(3) OVER (ORDER BY row_seq DESC, id) AS row_seq_bucket,
        NTILE(4) OVER w_part AS status_min_balance_avg_3_bucket,
        LAST_VALUE(balance_percent_rank) IGNORE NULLS OVER (PARTITION BY YEAR(opened_date) ORDER BY account_type_count_balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_balance_percent_rank
    FROM with_windows
    WINDOW w_part AS (ORDER BY status_min_balance_avg_3 DESC, id)
)
SELECT
    id,
    balance,
    opened_date,
    status_min_balance_avg_3_prior,
    status_min_balance_avg_3,
    prev_status_min_balance_avg_3_prior,
    row_seq_bucket,
    status_min_balance_avg_3_bucket,
    last_known_balance_percent_rank
FROM metrics
ORDER BY status_min_balance_avg_3_bucket DESC NULLS FIRST, opened_date DESC
