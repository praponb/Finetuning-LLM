WITH base AS (
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
        COALESCE(ms_played, 0) AS ms_played_filled
    FROM plays
    WHERE device = 'phone'
),
with_parent AS (
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
        t.bpm AS track_bpm,
        t.moods AS track_moods,
        t.artist_id AS track_artist_id
    FROM base s
    JOIN tracks t ON t.id = s.track_id
    WHERE t.bpm >= 85
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY is_skipped, source ORDER BY played_at DESC), 3) AS played_at_cume_dist,
        SIZE(COLLECT_SET(source) OVER (PARTITION BY track_artist_id, track_id)) AS distinct_source_in_track_artist_id_track_id,
        NTILE(4) OVER (PARTITION BY is_skipped, source ORDER BY played_at DESC, id) AS played_at_quartile,
        ms_played - LAG(ms_played) OVER (PARTITION BY is_skipped, source ORDER BY played_at DESC, id) AS ms_played_change
    FROM with_parent
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        ABS(ms_played_change - -120000) AS ms_played_change_distance,
        ROUND(played_at_cume_dist * 0.9, 2) AS played_at_cume_dist_adj
    FROM metrics
    WHERE SIZE(track_moods) >= 2
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        track_id,
        ms_played,
        device,
        source,
        track_artist_id,
        played_at_cume_dist,
        ms_played_change,
        ms_played_change_distance,
        played_at_cume_dist_adj,
        DATEDIFF(DATE '2026-01-01', played_at) AS days_since_played_at,
        DATE_TRUNC('HOUR', played_at) AS played_at_hour
    FROM derived
    WHERE ms_played <= 282311
)
-- Final projection
SELECT
    id,
    track_artist_id,
    played_at_cume_dist,
    ms_played_change,
    ms_played_change_distance,
    played_at_cume_dist_adj,
    days_since_played_at,
    played_at_hour
FROM tagged
ORDER BY ms_played_change DESC, played_at_hour DESC
