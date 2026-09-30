WITH filtered AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price
    FROM stock_moves
    WHERE warehouse_id IS NOT NULL
),
with_prev AS (
    -- Previous moved_at per warehouse_id
    SELECT *, LAG(moved_at) OVER (PARTITION BY warehouse_id ORDER BY moved_at, id) AS prev_moved_at
    FROM filtered
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_moved_at IS NULL OR moved_at > prev_moved_at + INTERVAL 60 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY warehouse_id ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        warehouse_id,
        burst_id,
        MIN(moved_at) AS session_start,
        MAX(moved_at) AS session_end,
        COUNT(*) AS n_events,
        SUM(unit_price) AS session_unit_price,
        ROUND((UNIX_TIMESTAMP(MAX(moved_at)) - UNIX_TIMESTAMP(MIN(moved_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY warehouse_id, burst_id
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN duration_min >= 204.0 THEN 'large' WHEN duration_min >= 60.0 THEN 'medium' ELSE 'small' END AS duration_min_tier,
        ROUND(SUM(session_unit_price), 2) AS total_session_unit_price,
        ROUND(AVG(duration_min), 2) AS avg_duration_min,
        ROUND(SUM(n_events) / NULLIF(SUM(burst_id), 0), 3) AS n_events_to_burst_id_ratio,
        COUNT(*) AS cnt,
        ROUND(SUM(session_unit_price) / NULLIF(SUM(burst_id), 0), 3) AS session_unit_price_to_burst_id_ratio
    FROM per_session
    GROUP BY 1
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY duration_min_tier, cnt ORDER BY cnt ASC, duration_min_tier) AS seq_no,
        COUNT(*) OVER (PARTITION BY duration_min_tier) AS rows_in_duration_min_tier,
        ROUND(n_events_to_burst_id_ratio / NULLIF(SUM(n_events_to_burst_id_ratio) OVER w_duration_min_tier, 0), 4) AS n_events_to_burst_id_ratio_share_of_duration_min,
        ROUND(avg_duration_min - AVG(avg_duration_min) OVER (PARTITION BY duration_min_tier), 2) AS avg_duration_min_vs_duration_min_tier_avg,
        SUM(cnt) OVER (PARTITION BY duration_min_tier) AS duration_min_tier_sum_cnt
    FROM summary
    WINDOW w_duration_min_tier AS (PARTITION BY duration_min_tier)
)
SELECT
    duration_min_tier,
    n_events_to_burst_id_ratio,
    cnt,
    rows_in_duration_min_tier,
    avg_duration_min_vs_duration_min_tier_avg,
    duration_min_tier_sum_cnt
FROM metrics
ORDER BY rows_in_duration_min_tier DESC NULLS FIRST, avg_duration_min_vs_duration_min_tier_avg
