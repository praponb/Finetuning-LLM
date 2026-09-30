WITH scoped_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM plays
    WHERE is_skipped = TRUE
      AND played_at < TIMESTAMP '2025-03-02 10:00:00'
),
combined_details_track AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.device,
        s.is_skipped,
        s.source,
        t.artist_id AS track_artist_id,
        t.is_explicit AS track_is_explicit,
        l.plan AS listener_plan
    FROM scoped_plays s
    JOIN tracks t ON t.id = s.track_id
    JOIN listeners l ON l.id = s.listener_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        played_at,
        device,
        is_skipped,
        source,
        track_artist_id,
        track_is_explicit,
        listener_plan,
        MIN(ms_played) OVER (PARTITION BY device, track_id) AS device_track_id_min_ms_played,
        ROUND(ms_played - AVG(ms_played) OVER w_device_track_id, 2) AS ms_played_vs_device_track_id_avg
    FROM combined_details_track
    WINDOW w_device_track_id AS (PARTITION BY device, track_id)
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', played_at) AS played_at_month,
        listener_plan,
        source,
        ARRAY_SORT(COLLECT_SET(listener_plan)) AS listener_plan_list,
        ROUND(AVG(ms_played_vs_device_track_id_avg), 2) AS avg_ms_played_vs_device_track_id_avg,
        COUNT(*) FILTER (WHERE track_is_explicit = FALSE) AS n_track_is_explicit_false,
        ROUND(SUM(device_track_id_min_ms_played) / NULLIF(SUM(ms_played_vs_device_track_id_avg), 0), 3) AS device_track_id_min_ms_played_to_ms_played_vs_de
    FROM with_windows
    GROUP BY DATE_TRUNC('MONTH', played_at), listener_plan, source
)
-- Final projection
SELECT
    played_at_month,
    listener_plan_list,
    avg_ms_played_vs_device_track_id_avg,
    n_track_is_explicit_false,
    CONCAT_WS(': ', listener_plan, source) AS listener_plan_source_label,
    COALESCE(source, 'UNKNOWN') AS source_or_unknown
FROM grouped
