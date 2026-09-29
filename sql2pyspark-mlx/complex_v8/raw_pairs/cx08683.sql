WITH recent_plays AS (
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
        CONCAT(source, '#', CAST(track_id AS STRING)) AS source_ref,
        SUBSTRING(TRIM(source), 1, 3) AS source_prefix
    FROM plays
    WHERE ms_played <= 340114
),
combined_details_track AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.device,
        s.is_skipped,
        s.source_ref,
        s.source_prefix,
        t.moods AS track_moods,
        t.release_date AS track_release_date
    FROM recent_plays s
    LEFT JOIN tracks t ON t.id = s.track_id
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM combined_details_track
    WHERE ms_played < (SELECT MAX(ms_played) FROM combined_details_track)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT track_id, is_skipped, device, ms_played
    FROM without_matches
) src
PIVOT (
    SUM(ms_played)
    FOR device IN ('phone', 'speaker')
)
ORDER BY track_id, is_skipped
