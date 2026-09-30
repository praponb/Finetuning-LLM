WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        is_skipped,
        source,
        CASE WHEN source IN ('playlist', 'album') THEN 'group_a' WHEN source = 'radio' THEN 'group_b' ELSE 'other' END AS source_group,
        MONTH(played_at) AS played_at_month
    FROM plays
    WHERE ms_played < 340114
),
combined_details_track AS (
    -- Attach track attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.is_skipped,
        t.title AS track_title,
        t.bpm AS track_bpm
    FROM base s
    JOIN tracks t ON t.id = s.track_id
    WHERE t.bpm > 89
)
SELECT
    id,
    listener_id,
    track_id,
    ms_played,
    is_skipped,
    track_title,
    track_bpm,
    LOWER(TRIM(track_title)) AS track_title_clean,
    LOWER(REGEXP_REPLACE(TRIM(track_title), ' ', '_')) AS track_title_slug
FROM combined_details_track
ORDER BY track_id, track_title NULLS LAST, id
LIMIT 10
