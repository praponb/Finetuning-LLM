WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        method,
        amount,
        status,
        CONCAT(method, '/', status) AS method_status
    FROM payments
    WHERE paid_at > DATE '2023-05-16'
      AND order_id IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        method,
        status,
        method_status,
        ROUND(amount / SUM(amount) OVER (PARTITION BY method), 4) AS amount_share,
        ROW_NUMBER() OVER (PARTITION BY method ORDER BY amount DESC, id ASC) AS row_seq,
        ROUND(amount / SUM(amount) OVER (PARTITION BY status, method), 4) AS amount_share_2,
        AVG(amount) OVER (ORDER BY amount ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS amount_avg_6_prior
    FROM filtered
    WHERE amount BETWEEN 510 AND 1540
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        method,
        status,
        amount_share,
        row_seq,
        amount_share_2,
        amount_avg_6_prior
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status, method ORDER BY amount_share_2 ASC, id ASC) AS latest_rank
        FROM metrics
    ) x
    WHERE latest_rank = 1
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        method,
        status,
        row_seq,
        amount_share * amount_share_2 AS amount_share_times_amount_share_2,
        COALESCE(method, 'UNKNOWN') AS method_filled
    FROM deduped
    WHERE amount_share_2 >= 0.66
),
filtered_payments AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        method,
        status,
        row_seq,
        CAST(row_seq AS STRING) AS row_seq_str
    FROM cleaned
    WHERE method <> 'cash'
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        method,
        row_seq,
        row_seq_str,
        CONCAT(method, ' - ', status) AS method_status,
        COALESCE(method, 'DIRECT') AS method_filled
    FROM filtered_payments
    WHERE row_seq < 6
)
-- Multi-level aggregation
SELECT
    CASE WHEN row_seq >= 7 THEN 'LARGE' ELSE 'SMALL' END AS row_seq_tier,
    MAX(row_seq) AS max_row_seq,
    SUM(CASE WHEN method = 'cash' THEN row_seq ELSE 0 END) AS cash_row_seq,
    COUNT_IF(method = 'promptpay') AS promptpay_events,
    AVG(row_seq) AS avg_row_seq
FROM base
GROUP BY CASE WHEN row_seq >= 7 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY cash_row_seq DESC;
