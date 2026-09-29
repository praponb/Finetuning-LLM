WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN is_flagged THEN 'Yes' ELSE 'No' END AS is_flagged_label,
        DATEDIFF(CURRENT_DATE(), txn_time) / 365 AS years_since_txn_time,
        COALESCE(merchant, 'UNKNOWN') AS merchant_clean
    FROM transactions
    WHERE txn_time >= DATE '2023-10-24'
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) > INTERVAL '90' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM base
),
sessions_transactions AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        account_id,
        amount,
        txn_time,
        merchant,
        is_flagged,
        SUM(session_start_flag) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
joined AS (
    -- Attach accounts attributes
    SELECT
        src.*,
        b.status,
        b.opened_date,
        b.account_type
    FROM sessions_transactions src
    JOIN accounts b
        ON src.account_id = b.id
    WHERE b.status NOT IN ('CLOSED', 'FROZEN')
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        account_id,
        amount,
        is_flagged,
        status,
        opened_date,
        account_type,
        MIN(amount) OVER (PARTITION BY MONTH(txn_time), merchant) AS txn_time_month_min_amount,
        MAX(amount) OVER (PARTITION BY DATE_TRUNC('WEEK', txn_time), account_type ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS amount_max_90d,
        ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY opened_date, id) AS seq_no
    FROM joined
),
joined_transactions AS (
    -- Attach accounts attributes
    SELECT
        s.id,
        s.account_id,
        s.amount,
        s.is_flagged,
        s.status,
        s.txn_time_month_min_amount,
        s.seq_no,
        a.status AS account_status,
        a.opened_date AS account_opened_date,
        a.balance
    FROM enriched s
    JOIN accounts a
        ON s.account_id = a.id
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id, account_id
    FROM joined_transactions
    INTERSECT ALL
    SELECT id, account_id
    FROM transactions
    WHERE is_flagged = TRUE
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM joined_transactions s
    JOIN matched_keys k ON s.id = k.id AND s.account_id = k.account_id
),
with_dims AS (
    -- Attach accounts attributes
    SELECT a.*, p.status AS account_status_2
    FROM verified a
    JOIN accounts p
        ON a.account_id = p.id
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    status,
    metric_name,
    metric_value
FROM (
    SELECT
        status,
        balance,
        amount
    FROM with_dims
) src
UNPIVOT (
    metric_value FOR metric_name IN (balance, amount AS AMOUNT_VAL)
)
