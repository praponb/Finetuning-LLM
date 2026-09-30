WITH matched_keys AS (
    -- Set intersection on keys
    SELECT id, order_id
    FROM payments
    INTERSECT
    SELECT id, order_id
    FROM payments
    WHERE status = 'REFUNDED'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM payments s
    INNER JOIN matched_keys k ON s.id = k.id AND s.order_id = k.order_id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY status ORDER BY amount DESC NULLS LAST) AS amount_cume_dist,
        SUM(amount) OVER (ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS amount_sum_7d,
        ROUND(amount / SUM(amount) OVER (PARTITION BY status), 4) AS amount_share,
        CUME_DIST() OVER (PARTITION BY method ORDER BY amount DESC NULLS LAST) AS amount_cume_dist_2,
        SUM(amount) OVER (ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS amount_sum_7d_2
    FROM reconciled
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        amount,
        status,
        amount_cume_dist,
        amount_sum_7d,
        amount_share,
        amount_cume_dist_2,
        amount_sum_7d_2,
        ROUND(amount_sum_7d_2, 1) AS amount_sum_7d_2_rounded,
        amount_cume_dist_2 + amount_sum_7d AS amount_cume_dist_2_plus_amount_sum_7d,
        COALESCE(method, 'N/A') AS resolved_method
    FROM with_windows
    WHERE amount_cume_dist_2 > 0.75
),
summary AS (
    -- Grouped metrics
    SELECT
        CASE WHEN amount_sum_7d >= 7042 THEN 'HIGH' ELSE 'LOW' END AS amount_sum_7d_tier,
        order_id,
        COUNT_IF(status = 'SUCCESS') AS success_events,
        COUNT(*) AS event_count,
        COUNT(DISTINCT status) AS distinct_status
    FROM base
    GROUP BY CASE WHEN amount_sum_7d >= 7042 THEN 'HIGH' ELSE 'LOW' END, order_id
)
SELECT
    amount_sum_7d_tier,
    order_id,
    event_count,
    distinct_status
FROM summary
ORDER BY distinct_status NULLS FIRST
