WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(balance * 1.1, 2) AS balance_adjusted
    FROM accounts
    WHERE customer_id = 19
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        balance_adjusted,
        ROW_NUMBER() OVER (PARTITION BY account_type, status ORDER BY balance_adjusted, id) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY opened_date, id) AS row_seq_2,
        SUM(balance_adjusted) OVER w_grp AS cumulative_balance_adjusted,
        AVG(balance_adjusted) OVER (ORDER BY balance, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS balance_adjusted_avg_5_prior
    FROM valid
    WINDOW w_grp AS (PARTITION BY status, account_type ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY opened_date DESC, id) AS event_seq,
        NTILE(5) OVER (PARTITION BY customer_id ORDER BY balance, id) AS balance_bucket,
        AVG(balance) OVER (PARTITION BY customer_id, account_type ORDER BY row_seq, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS balance_avg_3,
        SUM(balance_adjusted_avg_5_prior) OVER (PARTITION BY customer_id ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance_adjusted_avg_5_prior,
        CUME_DIST() OVER (ORDER BY row_seq DESC) AS row_seq_cume_dist
    FROM metrics
)
-- Running and moving aggregates
SELECT
    id,
    customer_id,
    account_type,
    balance,
    opened_date,
    status,
    balance_adjusted,
    row_seq,
    row_seq_2,
    cumulative_balance_adjusted,
    balance_adjusted_avg_5_prior,
    event_seq,
    balance_bucket,
    balance_avg_3,
    running_balance_adjusted_avg_5_prior,
    row_seq_cume_dist,
    NTILE(3) OVER (PARTITION BY account_type ORDER BY row_seq_2 DESC, id) AS row_seq_2_bucket,
    AVG(row_seq) OVER (PARTITION BY status, account_type) AS status_avg_row_seq
FROM with_windows;
