WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        account_id,
        amount,
        txn_time,
        merchant,
        is_flagged,
        CONCAT_WS('/', merchant, txn_type) AS merchant_txn_type_key,
        COALESCE(merchant, 'NONE') AS merchant_clean
    FROM transactions
    WHERE txn_time >= DATE '2023-06-21'
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        amount,
        txn_time,
        merchant,
        is_flagged,
        merchant_clean,
        TO_DATE(txn_time) AS txn_time_date
    FROM cleaned
    WHERE account_id IN (9, 17, 26)
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        merchant,
        is_flagged,
        txn_time_date,
        AVG(amount) OVER (ORDER BY amount, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS amount_avg_3,
        NTILE(4) OVER (PARTITION BY MONTH(txn_time) ORDER BY amount, id) AS amount_bucket,
        SUM(amount) OVER (PARTITION BY merchant ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount
    FROM valid
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        merchant,
        is_flagged,
        amount_avg_3,
        amount_bucket,
        cumulative_amount,
        COUNT(amount_bucket) OVER w_recent AS merchant_count_amount_bucket,
        SUM(cumulative_amount) OVER (PARTITION BY DATE_TRUNC('YEAR', txn_time_date), merchant ORDER BY cumulative_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cumulative_amount,
        AVG(cumulative_amount) OVER w_recent AS merchant_avg_cumulative_amount,
        SUM(cumulative_amount) OVER (ORDER BY amount_avg_3, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cumulative_amount_2,
        SUM(amount_bucket) OVER (PARTITION BY YEAR(txn_time_date), merchant ORDER BY amount_avg_3, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount_bucket
    FROM with_windows
    WINDOW w_recent AS (PARTITION BY merchant)
)
SELECT
    id,
    merchant,
    is_flagged,
    amount_avg_3,
    amount_bucket,
    cumulative_amount,
    merchant_count_amount_bucket,
    cumulative_cumulative_amount,
    merchant_avg_cumulative_amount,
    CASE WHEN merchant RLIKE '^(Starbucks|Amazon)$' THEN 'GROUP_B' ELSE 'PRIORITY' END AS merchant_zone
FROM enriched
ORDER BY amount_bucket DESC NULLS LAST, merchant DESC
