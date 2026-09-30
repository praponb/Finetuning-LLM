WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        TRIM(merchant) AS clean_merchant,
        CONCAT_WS(',', merchant, txn_type) AS merchant_txn_type_key,
        COALESCE(merchant, 'OTHER') AS resolved_merchant
    FROM transactions
    WHERE account_id IN (21, 23, 24, 25)
),
joined AS (
    -- Attach accounts attributes
    SELECT
        e.*,
        a.balance,
        a.account_type
    FROM valid e
    LEFT JOIN accounts a
        ON e.account_id = a.id
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM joined
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        account_id,
        merchant,
        clean_merchant,
        merchant_txn_type_key,
        balance,
        account_type,
        MAX(balance) OVER w_recent AS running_max_balance,
        MIN(balance) OVER w_recent AS running_min_balance,
        AVG(balance) OVER w_recent AS running_avg_balance,
        LAG(amount, 1) OVER (PARTITION BY txn_type, account_type ORDER BY txn_time, id) AS prev_amount
    FROM islands
    WINDOW w_recent AS (PARTITION BY account_id ORDER BY session_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Attach accounts attributes
    SELECT
        src.*,
        a.opened_date,
        a.balance AS account_balance,
        a.status
    FROM with_windows src
    LEFT JOIN accounts a
        ON src.account_id = a.id
    WHERE a.account_type IN ('CREDIT', 'CHECKING', 'SAVINGS')
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY running_max_balance DESC, id) AS latest_rank
        FROM enriched
    ) t
    WHERE latest_rank = 1
)
-- Multi-level aggregation
SELECT
    CASE WHEN running_min_balance >= 70000 THEN 'PREMIUM' ELSE 'STANDARD' END AS running_min_balance_tier,
    DATE_TRUNC('YEAR', opened_date) AS year_start,
    SUM(CASE WHEN status = 'FROZEN' THEN balance ELSE 0 END) AS frozen_balance,
    AVG(running_max_balance) FILTER (WHERE status = 'CLOSED') AS closed_avg_running_max_balance,
    SIZE(COLLECT_SET(status)) AS num_status,
    COUNT(CASE WHEN status = 'CLOSED' THEN 1 END) AS closed_count
FROM latest
GROUP BY ROLLUP (CASE WHEN running_min_balance >= 70000 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('YEAR', opened_date))
HAVING SUM(CASE WHEN status = 'FROZEN' THEN balance ELSE 0 END) > 0
