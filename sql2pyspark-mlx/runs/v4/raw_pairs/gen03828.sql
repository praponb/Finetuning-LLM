WITH kept_keys AS (
    -- Set intersection on keys
    SELECT order_id, status
    FROM payments
    INTERSECT
    SELECT order_id, status
    FROM payments
    WHERE paid_at < '2025-09-01'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM payments s
    INNER JOIN kept_keys k ON s.order_id = k.order_id AND s.status = k.status
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        CUME_DIST() OVER (PARTITION BY order_id, status ORDER BY amount DESC NULLS FIRST) amount_cume_dist,
        COUNT(amount) OVER (PARTITION BY status ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) amount_count_30d
    FROM kept_rows
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        SUBSTRING(status, 1, 2) status_prefix,
        ROUND(amount_count_30d, 2) amount_count_30d_rounded
    FROM metrics
    WHERE amount_cume_dist < 0.49
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id, order_id
    FROM cleaned
    INTERSECT ALL
    SELECT id, order_id
    FROM payments
    WHERE order_id IN (9, 14, 31, 32)
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN matched_keys k ON s.id = k.id AND s.order_id = k.order_id
)
-- Grouped metrics
SELECT
    CASE WHEN amount >= 1230 THEN 'PREMIUM' ELSE 'STANDARD' END AS amount_tier,
    PERCENTILE_APPROX(amount_cume_dist, ARRAY(0.25, 0.5, 0.75)) AS amount_cume_dist_quartiles,
    ROUND(SUM(amount_cume_dist) / NULLIF(COUNT(CASE WHEN method = 'paypal' THEN 1 END), 0), 2) AS amount_cume_dist_per_paypal,
    COUNT(*) AS row_count
FROM reconciled
GROUP BY CASE WHEN amount >= 1230 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY row_count NULLS LAST
