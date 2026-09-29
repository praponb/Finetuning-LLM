WITH filtered AS (
    -- Scope the plays rows
    SELECT *
    FROM plays
    WHERE ms_played BETWEEN 92277 AND 287769
      AND track_id IS NOT NULL
),
with_parent AS (
    -- Attach track attributes
    SELECT s.*, t.release_date AS track_release_date
    FROM filtered s
    JOIN tracks t ON t.id = s.track_id
    WHERE t.release_date IS NOT NULL
),
combined_details AS (
    -- Attach track attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.device,
        s.is_skipped,
        s.source,
        t.duration_sec AS track_duration_sec,
        t.release_date AS track_release_date_2
    FROM with_parent s
    LEFT JOIN tracks t ON t.id = s.track_id
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        ms_played,
        source,
        track_duration_sec,
        SIZE(COLLECT_SET(device) OVER w_listener_id) AS distinct_device_in_listener_id,
        ms_played - LAG(ms_played) OVER w_source_track_id_by_played_at AS ms_played_change
    FROM combined_details
    WINDOW w_listener_id AS (PARTITION BY listener_id), w_source_track_id_by_played_at AS (PARTITION BY source, track_id ORDER BY played_at, id)
),
play_stats AS (
    -- Per-listener plays metrics
    SELECT listener_id, MIN(played_at) AS first_played_at
    FROM plays
    GROUP BY listener_id
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.first_played_at
    FROM with_trends s
    LEFT JOIN play_stats p ON p.listener_id = s.listener_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        listener_id,
        source,
        track_duration_sec,
        distinct_device_in_listener_id,
        DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', first_played_at), distinct_device_in_listener_id ORDER BY distinct_device_in_listener_id) AS distinct_device_in_listener_id_dense_rank,
        ms_played - LAG(ms_played) OVER w_first_played_at_month_distin_2 AS ms_played_change_2,
        LAG(track_duration_sec, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', first_played_at), distinct_device_in_listener_id ORDER BY distinct_device_in_listener_id, id) AS prev2_track_duration_sec,
        MAX(ms_played_change) OVER (PARTITION BY distinct_device_in_listener_id) AS distinct_device_in_listener_id_max_ms_played_cha,
        LAST_VALUE(track_duration_sec) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', first_played_at), distinct_device_in_listener_id ORDER BY distinct_device_in_listener_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_track_duration_sec
    FROM profiled
    WINDOW w_first_played_at_month_distin_2 AS (PARTITION BY DATE_TRUNC('MONTH', first_played_at), distinct_device_in_listener_id ORDER BY distinct_device_in_listener_id, id)
),
with_activity AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM windowed s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND p.is_skipped = FALSE
              AND MONTH(p.played_at) IN (1, 2)
        )
)
SELECT
    id,
    listener_id,
    source,
    track_duration_sec,
    distinct_device_in_listener_id_dense_rank,
    ms_played_change_2,
    distinct_device_in_listener_id_max_ms_played_cha,
    last_known_track_duration_sec,
    CASE WHEN source IN ('search', 'radio') THEN 'group_a' WHEN source = 'playlist' THEN 'group_b' ELSE 'other' END AS source_group
FROM with_activity
ORDER BY listener_id DESC, source_group
