WITH cleaned_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped
    FROM plays
    WHERE is_skipped = FALSE
      AND device = 'speaker'
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.is_skipped,
        l.username AS listener_username,
        l.country AS listener_country
    FROM cleaned_plays s
    JOIN listeners l ON l.id = s.listener_id
),
with_parent AS (
    -- Attach listener attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.is_skipped,
        s.listener_username,
        s.listener_country,
        l.username AS listener_username_2,
        t.bpm AS track_bpm
    FROM joined s
    JOIN listeners l ON l.id = s.listener_id
    LEFT JOIN tracks t ON t.id = s.track_id
)
-- Final projection
SELECT
    id,
    track_id,
    is_skipped,
    listener_username,
    listener_country,
    listener_username_2,
    track_bpm,
    FLOOR(track_bpm / 25) * 25 AS track_bpm_bucket
FROM with_parent
ORDER BY listener_username NULLS LAST, track_bpm_bucket DESC
