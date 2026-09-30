WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LENGTH(txn_type) AS txn_type_length,
        COALESCE(merchant, 'NONE') AS merchant_filled
    FROM transactions
    WHERE merchant IS NOT NULL
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type, merchant ORDER BY txn_type_length, id) AS dedup_rank
    FROM valid
    QUALIFY dedup_rank = 1
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY merchant, txn_type ORDER BY amount, id) AS amount_bucket,
        PERCENT_RANK() OVER (ORDER BY txn_type_length DESC) AS txn_type_length_percent_rank,
        SUM(amount) OVER (ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS amount_sum_7d
    FROM latest
),
latest_transactions AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY txn_type_length_percent_rank DESC NULLS LAST, id) AS row_num
        FROM scored
    ) x
    WHERE row_num = 1
)
SELECT
    id,
    amount,
    merchant,
    txn_type_length,
    merchant_filled,
    dedup_rank,
    txn_type_length_percent_rank,
    amount_sum_7d,
    CASE WHEN merchant RLIKE '^(Amazon|Lazada)$' THEN 'CORE' WHEN merchant RLIKE '^(Grab|7-Eleven)$' THEN 'EXTENDED' ELSE 'OTHER_ZONE' END AS merchant_zone
FROM latest_transactions;
