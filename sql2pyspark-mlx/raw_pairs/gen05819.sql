WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATEDIFF(CURRENT_DATE(), txn_time) AS days_since_txn_time
    FROM transactions
    WHERE txn_time >= DATE '2023-08-28'
      AND merchant IS DISTINCT FROM 'Starbucks'
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY days_since_txn_time DESC NULLS FIRST, id) AS rn
    FROM base
    QUALIFY rn = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        amount,
        txn_time,
        merchant,
        is_flagged,
        rn,
        LAST_VALUE(merchant, TRUE) OVER (ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_merchant,
        CUME_DIST() OVER (PARTITION BY txn_type ORDER BY amount) AS amount_cume_dist,
        LAG(amount, 2) OVER (PARTITION BY account_id ORDER BY amount, id) AS prev_amount
    FROM latest
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        amount,
        txn_time,
        merchant,
        rn,
        last_known_merchant,
        amount_cume_dist,
        prev_amount,
        ROUND(amount, 2) AS amount_rounded,
        prev_amount + rn AS prev_amount_plus_rn,
        COALESCE(merchant, 'OTHER') AS merchant_clean
    FROM enriched
    WHERE amount BETWEEN 2500 AND 2700
      AND txn_time >= TO_TIMESTAMP('2023-05-02 04:00:00')
)
-- Ranking and gap features
SELECT
    id,
    merchant,
    merchant_clean,
    PERCENT_RANK() OVER (PARTITION BY merchant ORDER BY prev_amount DESC NULLS LAST) AS prev_amount_percent_rank,
    PERCENT_RANK() OVER (PARTITION BY merchant ORDER BY amount NULLS FIRST) AS amount_percent_rank,
    DENSE_RANK() OVER (PARTITION BY merchant ORDER BY prev_amount_plus_rn) AS prev_amount_plus_rn_dense_rank,
    NTILE(10) OVER (PARTITION BY DATE_TRUNC('MONTH', txn_time), merchant ORDER BY prev_amount, id) AS prev_amount_bucket
FROM filtered
WHERE amount_cume_dist BETWEEN 0.68 AND 0.74;
