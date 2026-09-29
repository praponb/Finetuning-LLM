WITH valid AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        DATEDIFF(DATE '2026-01-01', played_at) AS days_since_played_at,
        UPPER(TRIM(device)) AS device_clean,
        DATE_FORMAT(played_at, 'yyyy-MM-dd HH:00') AS played_at_label
    FROM plays
    WHERE is_skipped = TRUE
      AND ms_played >= 145720
),
with_parent AS (
    -- Attach track attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device,
        s.is_skipped,
        s.days_since_played_at,
        s.played_at_label,
        t.artist_id AS track_artist_id
    FROM valid s
    JOIN tracks t ON t.id = s.track_id
    WHERE t.artist_id IS NOT NULL
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        days_since_played_at,
        track_artist_id,
        FLOOR(days_since_played_at / 100) * 100 AS days_since_played_at_bucket
    FROM with_parent
    WHERE days_since_played_at < 595
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    device,
    days_since_played_at,
    days_since_played_at_bucket,
    DATE_TRUNC('WEEK', played_at) AS played_at_week,
    FLOOR(days_since_played_at / 100) * 100 AS days_since_played_at_bucket_2
FROM labeled
ORDER BY played_at DESC NULLS FIRST
