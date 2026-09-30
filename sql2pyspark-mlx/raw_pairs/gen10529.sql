WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        ABS(balance) AS abs_balance
    FROM accounts
    WHERE customer_id IS NOT NULL
      AND opened_date > DATE '2023-05-01'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        STDDEV_SAMP(balance) OVER w_part AS status_stddev_samp_balance,
        MAX(balance) OVER w_part AS status_max_balance,
        CUME_DIST() OVER (ORDER BY abs_balance DESC NULLS LAST) AS abs_balance_cume_dist,
        FIRST_VALUE(status) IGNORE NULLS OVER (PARTITION BY customer_id, account_type ORDER BY opened_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_status
    FROM base
    WINDOW w_part AS (PARTITION BY status)
),
matched_keys AS (
    -- Set difference on keys
    SELECT status, customer_id
    FROM with_windows
    EXCEPT DISTINCT
    SELECT status, customer_id
    FROM accounts
    WHERE status IN ('OPEN', 'FROZEN', 'CLOSED')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN matched_keys k ON s.status = k.status AND s.customer_id = k.customer_id
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM kept_rows s
    WHERE EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time <= s.opened_date
        )
),
kept_accounts AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.txn_time < s.opened_date
        )
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept_accounts s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.txn_type = 'FEE'
        )
)
SELECT
    id,
    customer_id,
    account_type,
    balance,
    opened_date,
    abs_balance,
    status_stddev_samp_balance,
    status_max_balance,
    abs_balance_cume_dist,
    CASE WHEN account_type RLIKE '^(CHECKING|SAVINGS)$' THEN 'OTHER_ZONE' WHEN account_type RLIKE '^(CREDIT|LOAN)$' THEN 'GROUP_A' ELSE 'EXTENDED' END AS account_type_zone
FROM not_flagged
