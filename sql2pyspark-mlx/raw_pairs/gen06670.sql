WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN account_type = 'LOAN' THEN 1 ELSE 0 END is_loan
    FROM accounts
    WHERE status IN ('CLOSED', 'FROZEN', 'OPEN')
),
kept_keys AS (
    -- Set intersection on keys
    SELECT account_type, id
    FROM cleaned
    INTERSECT ALL
    SELECT account_type, id
    FROM accounts
    WHERE status = 'OPEN'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN kept_keys k ON s.account_type = k.account_type AND s.id = k.id
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        balance,
        opened_date,
        status,
        DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY is_loan DESC) is_loan_dense_rank,
        RANK() OVER (PARTITION BY MONTH(opened_date), account_type ORDER BY balance DESC) balance_rank,
        STDDEV_SAMP(balance) OVER (PARTITION BY customer_id, status) customer_id_stddev_samp_balance,
        AVG(is_loan) OVER (PARTITION BY account_type) account_type_avg_is_loan
    FROM kept_rows
),
scored_accounts AS (
    -- Running and moving aggregates
    SELECT
        id,
        opened_date,
        is_loan_dense_rank,
        balance_rank,
        customer_id_stddev_samp_balance,
        MAX(is_loan_dense_rank) OVER (PARTITION BY MONTH(opened_date)) opened_date_month_max_is_loan_dense_rank,
        STDDEV_SAMP(balance_rank) OVER (PARTITION BY status) status_stddev_samp_balance_rank,
        CUME_DIST() OVER (PARTITION BY YEAR(opened_date), status ORDER BY balance DESC) balance_cume_dist
    FROM scored
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored_accounts s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.merchant IN ('Amazon', 'Shell')
        )
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM kept
    INTERSECT
    SELECT id
    FROM accounts
    WHERE status != 'FROZEN'
),
kept_rows_accounts AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept s
    INNER JOIN remaining_keys k ON s.id = k.id
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN balance_cume_dist >= 0.72 THEN 'PREMIUM' ELSE 'STANDARD' END AS balance_cume_dist_tier,
    ROUND(AVG(is_loan_dense_rank), 2) AS avg_is_loan_dense_rank,
    ROUND(AVG(balance_rank), 2) AS avg_balance_rank,
    COUNT(*) AS num_rows
FROM kept_rows_accounts
GROUP BY balance_cume_dist_tier
HAVING avg_is_loan_dense_rank < 1000000
