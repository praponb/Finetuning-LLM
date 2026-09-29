WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        status,
        COALESCE(balance, 0) AS balance_filled,
        COALESCE(status, 'NONE') AS resolved_status
    FROM accounts
    WHERE status IS NOT DISTINCT FROM 'CLOSED'
      AND account_type = 'CHECKING'
),
latest_match AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.account_type,
        s.status,
        s.balance_filled,
        latest.txn_type
    FROM base s
    JOIN LATERAL (
            SELECT c.txn_type
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM latest_match s
    WHERE EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND TO_DATE(c.txn_time) < '2023-07-01'
        )
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        account_type,
        FIRST_VALUE(status, TRUE) OVER w_hist AS first_known_status,
        LAG(balance_filled, 1) OVER (PARTITION BY status ORDER BY balance_filled, id) AS prev_balance_filled
    FROM kept
    WHERE status = 'OPEN'
    WINDOW w_hist AS (PARTITION BY txn_type ORDER BY balance_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
kept_accounts AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.amount <= 2700
        )
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN prev_balance_filled >= 66 THEN 'HIGH' ELSE 'LOW' END AS prev_balance_filled_tier,
        MAX(prev_balance_filled) FILTER (WHERE account_type = 'CHECKING') AS checking_max_prev_balance_filled,
        COUNT(DISTINCT id) AS distinct_ids,
        SIZE(COLLECT_SET(account_type)) AS num_account_types
    FROM kept_accounts
    GROUP BY CASE WHEN prev_balance_filled >= 66 THEN 'HIGH' ELSE 'LOW' END
)
SELECT
    prev_balance_filled_tier,
    checking_max_prev_balance_filled,
    distinct_ids,
    num_account_types
FROM summary
ORDER BY num_account_types DESC, prev_balance_filled_tier
