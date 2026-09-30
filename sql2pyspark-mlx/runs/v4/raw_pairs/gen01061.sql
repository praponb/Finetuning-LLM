WITH filtered AS (
    SELECT
        *,
        ABS(balance) AS abs_balance,
        COALESCE(status, 'DIRECT') AS resolved_status
    FROM accounts
    WHERE status IS NOT NULL
      AND account_type NOT IN ('CREDIT', 'CHECKING', 'LOAN')
),
base AS (
    SELECT
        id,
        account_type,
        balance,
        opened_date,
        status,
        resolved_status,
        SUBSTRING(status, 1, 4) AS status_prefix,
        DAYOFWEEK(opened_date) AS opened_date_dayofweek,
        COALESCE(status, 'DIRECT') AS resolved_status_2
    FROM filtered
    WHERE opened_date > DATE '2024-02-13'
      AND status IS NOT NULL
),
not_flagged AS (
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.txn_time <= s.opened_date
        )
),
with_windows AS (
    SELECT
        id,
        account_type,
        balance,
        opened_date,
        status,
        resolved_status,
        status_prefix,
        opened_date_dayofweek,
        resolved_status_2,
        STDDEV_SAMP(balance) OVER (PARTITION BY status) AS status_stddev_samp_balance,
        MIN(opened_date_dayofweek) OVER (PARTITION BY account_type) AS account_type_min_opened_date_dayofweek,
        AVG(opened_date_dayofweek) OVER (PARTITION BY status ORDER BY opened_date, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS opened_date_dayofweek_avg_5
    FROM not_flagged
),
eligible AS (
    SELECT s.*
    FROM with_windows s
    WHERE EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.txn_time < s.opened_date
        )
),
scored AS (
    SELECT
        *,
        SUM(opened_date_dayofweek_avg_5) OVER (PARTITION BY status ORDER BY opened_date_dayofweek, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_opened_date_dayofweek_avg_5,
        ROW_NUMBER() OVER (ORDER BY opened_date_dayofweek_avg_5 DESC, id) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY balance, id) AS row_seq,
        AVG(opened_date_dayofweek_avg_5) OVER (ORDER BY opened_date_dayofweek, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS opened_date_dayofweek_avg_5_avg_2_prior,
        SUM(status_stddev_samp_balance) OVER (ORDER BY status_stddev_samp_balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_status_stddev_samp_balance
    FROM eligible
),
eligible_accounts AS (
    SELECT s.*
    FROM scored s
    WHERE EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time <= s.opened_date
        )
)
SELECT
    id,
    account_type,
    balance,
    opened_date,
    status,
    resolved_status,
    status_prefix,
    opened_date_dayofweek,
    account_type_min_opened_date_dayofweek,
    CASE WHEN status RLIKE '^(OPEN|FROZEN)$' THEN 'CORE' WHEN status RLIKE '^(CLOSED)$' THEN 'PRIORITY' ELSE 'GROUP_A' END AS status_zone
FROM eligible_accounts
