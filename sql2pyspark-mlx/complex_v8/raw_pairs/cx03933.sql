WITH prepared_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        DATE_TRUNC('YEAR', played_at) AS played_at_year
    FROM plays
    WHERE source IS NOT NULL
),
with_prev AS (
    -- Previous played_at per source
    SELECT *, LAG(played_at) OVER (PARTITION BY source ORDER BY played_at, id) AS prev_played_at
    FROM prepared_plays
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR (UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(prev_played_at)) / 60 > 30 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY source ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        source,
        burst_id,
        MIN(played_at) AS session_start,
        MAX(played_at) AS session_end,
        COUNT(*) AS n_events,
        SUM(ms_played) AS session_ms_played,
        ROUND((UNIX_TIMESTAMP(MAX(played_at)) - UNIX_TIMESTAMP(MIN(played_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY source, burst_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        source,
        burst_id,
        session_start,
        session_ms_played,
        duration_min,
        n_events - LAG(n_events) OVER (ORDER BY n_events, source, burst_id) AS n_events_change,
        RANK() OVER w_all_by_n_events AS n_events_rank,
        LAST_VALUE(session_start) OVER (ORDER BY session_end DESC, source, burst_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_session_start,
        ROUND(AVG(burst_id) OVER (PARTITION BY source, burst_id), 2) AS source_burst_id_avg_burst_id
    FROM per_session
    WINDOW w_all_by_n_events AS (ORDER BY n_events, source, burst_id)
),
tagged AS (
    -- Add calculated columns
    SELECT
        source,
        burst_id,
        session_start,
        session_ms_played,
        duration_min,
        n_events_change,
        last_session_start,
        source_burst_id_avg_burst_id,
        CASE WHEN n_events_rank >= 8 THEN 'A' WHEN n_events_rank >= 4 THEN 'B' ELSE 'C' END AS n_events_rank_tier,
        DATE_TRUNC('QUARTER', last_session_start) AS last_session_start_quarter
    FROM windowed
)
SELECT
    source,
    burst_id,
    session_start,
    session_ms_played,
    duration_min,
    n_events_change,
    last_session_start,
    source_burst_id_avg_burst_id,
    CONCAT(n_events_rank_tier, '#', CAST(session_ms_played AS STRING)) AS n_events_rank_tier_ref,
    CONCAT_WS(' / ', source, n_events_rank_tier) AS source_n_events_rank_tier_label
FROM tagged
ORDER BY source_n_events_rank_tier_label, n_events_rank_tier_ref DESC, source, burst_id
LIMIT 10
