WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(amount, 0) AS amount_filled,
        COALESCE(merchant, 'N/A') AS merchant_clean
    FROM transactions
    WHERE txn_time >= TO_DATE('2023-10-24')
      AND merchant IS DISTINCT FROM 'Lazada'
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY txn_time DESC NULLS FIRST, id) AS rn
    FROM valid
    QUALIFY rn = 1
),
joined AS (
    -- Attach accounts attributes
    SELECT
        src.id,
        src.account_id,
        src.txn_type,
        src.is_flagged,
        src.amount_filled,
        src.merchant_clean,
        src.rn,
        p.opened_date,
        p.account_type,
        p.status
    FROM deduped src
    LEFT JOIN accounts p
        ON src.account_id = p.id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(amount_filled) OVER w_part AS last4_avg_amount_filled,
        MAX(amount_filled) OVER w_part AS last4_max_amount_filled,
        SUM(amount_filled) OVER w_part AS last4_sum_amount_filled,
        AVG(amount_filled) OVER (PARTITION BY status, txn_type ORDER BY opened_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS amount_filled_avg_3
    FROM joined
    WHERE status IN ('CLOSED', 'OPEN')
    WINDOW w_part AS (PARTITION BY account_id ORDER BY amount_filled, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
daily AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN last4_avg_amount_filled >= 74 THEN 'HIGH' ELSE 'LOW' END AS last4_avg_amount_filled_tier,
        account_type,
        account_id,
        PERCENTILE_APPROX(amount_filled_avg_3, 0.9) AS amount_filled_avg_3_p90,
        ROUND(AVG(last4_avg_amount_filled), 2) AS avg_last4_avg_amount_filled,
        APPROX_PERCENTILE(last4_avg_amount_filled, 0.95) AS last4_avg_amount_filled_p95,
        SORT_ARRAY(COLLECT_SET(account_type)) AS account_type_list,
        COUNT(CASE WHEN txn_type = 'REFUND' THEN 1 END) AS refund_count,
        GROUPING_ID() AS grouping_level
    FROM with_windows
    GROUP BY GROUPING SETS ((CASE WHEN last4_avg_amount_filled >= 74 THEN 'HIGH' ELSE 'LOW' END, account_type, account_id), (CASE WHEN last4_avg_amount_filled >= 74 THEN 'HIGH' ELSE 'LOW' END, account_type), (CASE WHEN last4_avg_amount_filled >= 74 THEN 'HIGH' ELSE 'LOW' END), ())
)
SELECT
    s.account_type,
    s.account_id,
    s.amount_filled_avg_3_p90,
    s.avg_last4_avg_amount_filled,
    s.last4_avg_amount_filled_p95,
    s.refund_count,
    (SELECT MIN(t2.amount) FROM transactions t2 WHERE t2.account_id = s.account_id) AS min_transaction_amount
FROM daily s
ORDER BY account_type;
