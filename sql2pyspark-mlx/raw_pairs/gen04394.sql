WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        CAST(is_flagged AS INT) is_flagged_flag,
        TO_DATE(txn_time) txn_time_date
    FROM transactions
    WHERE merchant IS NOT NULL
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(amount) OVER (PARTITION BY txn_type ORDER BY txn_time_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_avg_amount,
        SUM(amount) OVER (PARTITION BY txn_type ORDER BY txn_time_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_sum_amount,
        MAX(amount) OVER (PARTITION BY txn_type ORDER BY txn_time_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_max_amount,
        MIN(amount) OVER (PARTITION BY txn_type ORDER BY txn_time_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_min_amount
    FROM cleaned
),
joined AS (
    -- Attach accounts attributes
    SELECT s.*, p.opened_date
    FROM metrics s
    JOIN accounts p
        ON s.account_id = p.id
    WHERE p.customer_id IS NOT NULL
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(amount) OVER (PARTITION BY merchant ORDER BY is_flagged_flag, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_count_amount,
        MAX(amount) OVER (PARTITION BY merchant ORDER BY is_flagged_flag, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_max_amount_2,
        AVG(amount) OVER (PARTITION BY merchant ORDER BY is_flagged_flag, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_avg_amount_2,
        LAST_VALUE(amount) IGNORE NULLS OVER (PARTITION BY account_id, merchant ORDER BY txn_time_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_amount
    FROM joined
)
SELECT
    id,
    account_id,
    txn_time,
    merchant,
    txn_time_date,
    opened_date,
    running_max_amount_2,
    running_avg_amount_2,
    last_known_amount,
    CASE WHEN merchant RLIKE '^(7-Eleven|Amazon)$' THEN 'EXTENDED' WHEN merchant RLIKE '^(Grab|Lazada)$' THEN 'GROUP_B' ELSE 'GROUP_A' END AS merchant_zone
FROM enriched
ORDER BY id, account_id
LIMIT 10;
