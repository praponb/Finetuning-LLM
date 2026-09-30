WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ADD_MONTHS(opened_date, 6) AS opened_date_plus_months,
        COALESCE(status, 'OTHER') AS status_filled
    FROM accounts
    WHERE status IS NOT NULL
      AND account_type = 'SAVINGS'
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LENGTH(status) AS status_length,
        COALESCE(status, 'OTHER') AS status_filled_2
    FROM base
    WHERE opened_date >= DATE '2023-10-05'
      AND account_type IN ('CHECKING', 'SAVINGS')
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        FIRST_VALUE(balance) IGNORE NULLS OVER (PARTITION BY customer_id, status ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_balance,
        DENSE_RANK() OVER (ORDER BY balance DESC NULLS FIRST) AS balance_dense_rank,
        NTILE(3) OVER (ORDER BY balance DESC, id) AS balance_bucket,
        SUM(status_length) OVER (PARTITION BY status ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_status_length,
        DENSE_RANK() OVER (PARTITION BY account_type ORDER BY status_length NULLS FIRST) AS status_length_dense_rank
    FROM valid
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY status ORDER BY cumulative_status_length, id) AS cumulative_status_length_bucket,
        NTILE(4) OVER (PARTITION BY account_type ORDER BY balance, id) AS balance_bucket_2
    FROM with_windows
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.txn_time < s.opened_date_plus_months
        )
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('MONTH', opened_date) AS activity_month,
    DATE_TRUNC('QUARTER', opened_date_plus_months) AS quarter_start,
    COUNT(*) FILTER (WHERE account_type = 'SAVINGS') AS savings_rows,
    COUNT_IF(status = 'OPEN') AS open_events,
    SUM(status_length) AS total_status_length,
    DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', opened_date) ORDER BY COUNT_IF(status = 'OPEN') DESC) AS open_events_dense_rank
FROM not_flagged
GROUP BY DATE_TRUNC('MONTH', opened_date), DATE_TRUNC('QUARTER', opened_date_plus_months)
ORDER BY open_events NULLS LAST;
