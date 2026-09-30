WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        method,
        paid_at,
        FLOOR(amount) AS amount_whole,
        COALESCE(method, 'DIRECT') AS resolved_method
    FROM payments
    WHERE order_id IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER w_recent AS amount_whole_rank,
        LEAD(amount_whole, 1, 0) OVER w_part AS next_amount_whole
    FROM base
    WINDOW w_recent AS (PARTITION BY method ORDER BY amount_whole DESC NULLS LAST), w_part AS (ORDER BY paid_at, id)
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id, method
    FROM metrics
    INTERSECT ALL
    SELECT id, method
    FROM payments
    WHERE method != 'paypal'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.method = k.method
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        method,
        paid_at,
        amount_whole_rank,
        CASE WHEN amount_whole >= 82 THEN 'High' WHEN amount_whole >= 23 THEN 'Medium' ELSE 'Low' END AS amount_whole_band,
        COALESCE(method, 'DIRECT') AS method_clean
    FROM reconciled
    WHERE method IS NOT NULL
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, CAST(paid_at AS DATE) AS paid_at_day
    FROM valid
    WHERE paid_at < '2023-10-01'
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(amount_whole_rank * 0.9, 2) AS amount_whole_rank_scaled
    FROM cleaned
    WHERE paid_at_day >= '2024-11-07'
),
report AS (
    SELECT
        paid_at,
        amount_whole_band,
        method_clean,
        paid_at_day
    FROM filtered
    ORDER BY paid_at_day DESC
)
SELECT
    paid_at,
    amount_whole_band,
    method_clean
FROM report
ORDER BY paid_at DESC NULLS LAST;
