WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        SUBSTRING(TRIM(source), 1, 2) AS source_prefix
    FROM plays
    WHERE device IS NOT NULL
      AND played_at >= TIMESTAMP '2025-01-31 09:00:00'
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM plays p WHERE p.listener_id = s.listener_id) AS play_count
    FROM base s
),
with_details_track AS (
    -- Attach track attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.is_skipped,
        s.source_prefix,
        s.play_count,
        t.artist_id AS track_artist_id,
        t.is_explicit AS track_is_explicit,
        l.country AS listener_country
    FROM with_stats s
    JOIN tracks t ON t.id = s.track_id
    JOIN listeners l ON l.id = s.listener_id
    WHERE t.artist_id <= 12
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    ms_played,
    is_skipped,
    source_prefix,
    play_count,
    track_artist_id,
    track_is_explicit,
    listener_country,
    COALESCE(ms_played, 0) AS ms_played_filled,
    GREATEST(play_count, ms_played) AS greatest_play_count_ms_played
FROM with_details_track
WHERE play_count BETWEEN 1 AND 8
ORDER BY ms_played
