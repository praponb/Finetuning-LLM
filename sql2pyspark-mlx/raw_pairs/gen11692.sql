WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, DATE_FORMAT(opened_date, 'yyyy-MM-dd') AS opened_date_label
    FROM accounts
    WHERE opened_date > TO_DATE('2023-04-11')
      AND balance BETWEEN 22000 AND 47000
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(balance) OVER (PARTITION BY status ORDER BY opened_date, id) AS prev_balance,
        LEAD(balance) OVER (PARTITION BY status ORDER BY opened_date, id) AS next_balance,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY opened_date, id) AS row_seq,
        NTILE(10) OVER (PARTITION BY MONTH(opened_date), status ORDER BY balance DESC, id) AS balance_bucket
    FROM filtered
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(3) OVER w_running AS balance_bucket_bucket,
        LAG(next_balance, 2) OVER (ORDER BY prev_balance, id) AS prev_next_balance
    FROM metrics
    WHERE next_balance < 66000
    WINDOW w_running AS (PARTITION BY YEAR(opened_date), status ORDER BY balance_bucket DESC, id)
),
with_windows_accounts AS (
    -- Ranking and gap features
    SELECT
        id,
        balance,
        opened_date,
        opened_date_label,
        prev_balance,
        row_seq,
        balance_bucket,
        balance_bucket_bucket,
        LAG(balance) OVER (PARTITION BY account_type ORDER BY row_seq, id) AS prev_balance_2,
        LEAD(balance) OVER (PARTITION BY account_type ORDER BY row_seq, id) AS next_balance_2,
        LEAD(balance_bucket) OVER (PARTITION BY account_type ORDER BY row_seq, id) AS next_balance_bucket,
        ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY row_seq, id) AS event_seq
    FROM with_windows
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        balance,
        opened_date,
        prev_balance,
        row_seq,
        balance_bucket,
        prev_balance_2,
        next_balance_2,
        next_balance_bucket,
        event_seq,
        FLOOR(next_balance_2) AS next_balance_2_whole
    FROM with_windows_accounts
    WHERE opened_date >= DATE '2023-05-11'
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('WEEK', opened_date) AS week_start,
    DATE_TRUNC('YEAR', opened_date) AS year_start,
    CASE WHEN balance_bucket >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS balance_bucket_tier,
    COUNT(*) AS event_count,
    COUNT(*) AS num_rows,
    COUNT(*) AS num_rows_2,
    PERCENTILE_APPROX(prev_balance_2, ARRAY(0.25, 0.5, 0.75)) AS prev_balance_2_quartiles
FROM cleaned
GROUP BY 1, 2, 3
ORDER BY balance_bucket_tier DESC NULLS LAST;
