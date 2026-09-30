WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        account_id,
        txn_type,
        txn_time,
        merchant,
        is_flagged,
        TRIM(merchant) AS clean_merchant
    FROM transactions
    WHERE txn_time >= TIMESTAMP '2023-09-15 22:00:00'
),
joined AS (
    -- Attach accounts attributes
    SELECT
        s.*,
        a.balance,
        a.opened_date
    FROM cleaned s
    LEFT JOIN accounts a
        ON s.account_id = a.id
    WHERE a.customer_id = 28
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        account_id,
        txn_type,
        txn_time,
        merchant,
        is_flagged,
        clean_merchant,
        balance,
        opened_date,
        CASE WHEN txn_type IN ('CREDIT', 'DEBIT') THEN 'Group A' ELSE 'Other' END AS txn_type_group,
        COALESCE(merchant, 'DIRECT') AS resolved_merchant
    FROM joined
    WHERE txn_time > TO_TIMESTAMP('2023-07-28 22:00:00')
      AND merchant IS NOT NULL
),
cleaned_transactions AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        MONTH(opened_date) AS opened_date_month,
        DATE_TRUNC('WEEK', txn_time) AS txn_time_period,
        COALESCE(merchant, 'OTHER') AS merchant_filled
    FROM valid
    WHERE balance >= 37000
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time ASC, id ASC) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time ASC, id ASC) + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM cleaned_transactions
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY merchant ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('QUARTER', txn_time_period) AS quarter_start,
    CASE WHEN opened_date_month >= 78 THEN 'PREMIUM' ELSE 'STANDARD' END AS opened_date_month_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    SIZE(COLLECT_SET(txn_type)) AS num_txn_types
FROM sessionized
GROUP BY DATE_TRUNC('QUARTER', txn_time_period), CASE WHEN opened_date_month >= 78 THEN 'PREMIUM' ELSE 'STANDARD' END;
