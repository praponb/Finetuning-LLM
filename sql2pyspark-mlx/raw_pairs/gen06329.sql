WITH with_dims AS (
    -- Attach accounts attributes
    SELECT
        e.id,
        e.account_id,
        e.txn_type,
        e.txn_time,
        e.merchant,
        e.is_flagged,
        d.balance
    FROM transactions e
    LEFT JOIN accounts d
        ON e.account_id = d.id
    WHERE YEAR(d.opened_date) >= 2025
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        account_id,
        txn_type,
        merchant,
        is_flagged,
        LEAD(balance) OVER (PARTITION BY account_id, merchant ORDER BY balance, id) AS next_balance,
        LAG(balance) OVER (PARTITION BY account_id, merchant ORDER BY balance, id) AS prev_balance,
        ROW_NUMBER() OVER (PARTITION BY account_id, merchant ORDER BY balance, id) AS seq_no
    FROM with_dims
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        merchant,
        is_flagged,
        prev_balance
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY next_balance, id) AS rn
        FROM enriched
    ) t
    WHERE rn = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        is_flagged,
        LAG(prev_balance, 2) OVER (ORDER BY prev_balance, id) AS prev_prev_balance,
        MIN(prev_balance) OVER (PARTITION BY merchant) AS merchant_min_prev_balance
    FROM ranked
),
enriched_transactions AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(prev_prev_balance) OVER (ORDER BY merchant_min_prev_balance, id) AS prev_prev_prev_balance,
        LEAD(prev_prev_balance) OVER (ORDER BY merchant_min_prev_balance, id) AS next_prev_prev_balance,
        LEAD(merchant_min_prev_balance) OVER (ORDER BY merchant_min_prev_balance, id) AS next_merchant_min_prev_balance,
        ROW_NUMBER() OVER (ORDER BY merchant_min_prev_balance, id) AS row_seq
    FROM with_windows
)
-- Multi-level aggregation
SELECT
    CASE WHEN row_seq >= 7 THEN 'LARGE' ELSE 'SMALL' END AS row_seq_tier,
    CASE WHEN next_merchant_min_prev_balance >= 62575 THEN 'PREMIUM' ELSE 'STANDARD' END AS next_merchant_min_prev_balance_tier,
    AVG(row_seq) AS avg_row_seq,
    PERCENTILE_APPROX(merchant_min_prev_balance, 0.9) AS merchant_min_prev_balance_p90,
    COUNT(*) AS event_count,
    PERCENTILE_APPROX(row_seq, ARRAY(0.25, 0.5, 0.75)) AS row_seq_quartiles,
    ROUND(AVG(prev_prev_prev_balance), 2) AS avg_prev_prev_prev_balance
FROM enriched_transactions
GROUP BY CASE WHEN row_seq >= 7 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN next_merchant_min_prev_balance >= 62575 THEN 'PREMIUM' ELSE 'STANDARD' END;
