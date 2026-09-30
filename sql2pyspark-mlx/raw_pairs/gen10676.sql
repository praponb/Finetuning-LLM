WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END AS is_failed,
        COALESCE(method, 'UNKNOWN') AS method_filled
    FROM payments
    WHERE order_id IS NOT NULL
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        TRIM(method) AS clean_method,
        is_failed * amount AS is_failed_times_amount,
        COALESCE(method, 'N/A') AS resolved_method
    FROM cleaned
    WHERE paid_at >= TIMESTAMP '2023-09-24 14:00:00'
      AND method IS NOT NULL
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at ASC, id ASC) > INTERVAL '30' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM filtered
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY method ORDER BY paid_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM islands
    INTERSECT ALL
    SELECT id
    FROM payments
    WHERE paid_at >= '2024-05-30'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM islands s
    JOIN matched_keys k ON s.id = k.id
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(amount) OVER (PARTITION BY status ORDER BY CAST(paid_at AS BIGINT) ASC RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS amount_count_7d,
        SUM(amount) OVER (PARTITION BY order_id, method ORDER BY CAST(paid_at AS BIGINT) ASC RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS amount_sum_7d
    FROM reconciled
),
cleaned_payments AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN method = 'card' THEN 1 ELSE 0 END AS is_card
    FROM enriched
    WHERE paid_at >= DATE '2023-09-22'
      AND method IS NOT NULL
)
-- Join a dimension and aggregate
SELECT
    p.status AS order_status,
    p.channel,
    COUNT(*) AS cnt,
    MIN(s.island_id) AS min_island_id,
    GROUPING_ID() AS gid
FROM cleaned_payments s
JOIN orders p ON s.order_id = p.id
GROUP BY GROUPING SETS ((p.status, p.channel), (p.status), ())
