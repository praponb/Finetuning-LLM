WITH scoped_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        played_at + INTERVAL 30 MINUTES AS played_at_plus_30m,
        CONCAT(device, '#', CAST(listener_id AS STRING)) AS device_ref
    FROM plays
    WHERE ms_played > 145720
      AND source = 'playlist'
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        track_id,
        ms_played,
        device,
        is_skipped,
        played_at_plus_30m,
        ROUND((UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(LAG(played_at) OVER (ORDER BY ms_played ASC, id))) / 60, 1) AS minutes_since_prev_played_at,
        SIZE(COLLECT_SET(device) OVER w_source) AS distinct_device_in_source,
        ROUND(AVG(ms_played) OVER (ORDER BY ms_played ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS ms_played_avg_7,
        ROUND(AVG(ms_played) OVER (PARTITION BY source), 2) AS source_avg_ms_played,
        ROUND(PERCENT_RANK() OVER (ORDER BY ms_played ASC, id), 3) AS ms_played_percent_rank,
        SUM(ms_played) OVER (ORDER BY ms_played ASC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS ms_played_sum_2_prior,
        SUM(ms_played) OVER (ORDER BY ms_played ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ms_played,
        SUM(ms_played) OVER (PARTITION BY source) AS source_sum_ms_played
    FROM scoped_plays
    WINDOW w_source AS (PARTITION BY source)
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        track_id,
        device,
        is_skipped,
        played_at_plus_30m,
        source_avg_ms_played,
        ms_played_percent_rank,
        ms_played_sum_2_prior,
        running_ms_played,
        ROUND(ms_played_percent_rank * 1.1, 2) AS ms_played_percent_rank_adj
    FROM sequenced
),
totals AS (
    -- Group-level totals
    SELECT
        device,
        is_skipped,
        ROUND(100 * COUNT(CASE WHEN NOT is_skipped THEN 1 END) / COUNT(*), 1) AS pct_not_is_skipped,
        COUNT(*) FILTER (WHERE device <> 'car') AS n_device_car,
        ROUND(SUM(source_avg_ms_played) / NULLIF(SUM(ms_played_percent_rank_adj), 0), 3) AS source_avg_ms_played_to_ms_played_percent_rank_a,
        ROUND(SUM(running_ms_played) / NULLIF(SUM(ms_played_percent_rank), 0), 3) AS running_ms_played_to_ms_played_percent_rank_rati,
        MAX(played_at_plus_30m) AS last_played_at_plus_30m
    FROM enriched
    GROUP BY device, is_skipped
),
metrics AS (
    -- Previous / next values per key
    SELECT
        device,
        is_skipped,
        pct_not_is_skipped,
        source_avg_ms_played_to_ms_played_percent_rank_a,
        running_ms_played_to_ms_played_percent_rank_rati,
        ROUND(PERCENT_RANK() OVER (PARTITION BY device ORDER BY last_played_at_plus_30m ASC, device, is_skipped), 3) AS last_played_at_plus_30m_percent_rank,
        LAST_VALUE(last_played_at_plus_30m) IGNORE NULLS OVER (PARTITION BY device ORDER BY last_played_at_plus_30m ASC, device, is_skipped ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_last_played_at_plus_30m
    FROM totals
)
-- Final projection
SELECT
    device,
    is_skipped,
    pct_not_is_skipped,
    source_avg_ms_played_to_ms_played_percent_rank_a,
    running_ms_played_to_ms_played_percent_rank_rati,
    last_played_at_plus_30m_percent_rank,
    CASE WHEN is_skipped THEN 1 ELSE 0 END AS is_skipped_flag,
    IF(is_skipped, 'yes', 'no') AS is_skipped_label
FROM metrics
ORDER BY is_skipped_label DESC NULLS FIRST
