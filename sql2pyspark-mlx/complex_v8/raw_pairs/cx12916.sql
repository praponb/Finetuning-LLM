WITH recent_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        DATE_TRUNC('MONTH', played_at) AS played_at_month
    FROM plays
    WHERE listener_id IS NOT NULL
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.is_skipped,
        t.title AS track_title,
        t.release_date AS track_release_date
    FROM recent_plays s
    JOIN tracks t ON t.id = s.track_id
)
SELECT
    id,
    track_id,
    played_at,
    ms_played,
    track_release_date,
    CONCAT(track_title, '#', CAST(ms_played AS STRING)) AS track_title_ref
FROM combined_details
ORDER BY ms_played NULLS LAST
