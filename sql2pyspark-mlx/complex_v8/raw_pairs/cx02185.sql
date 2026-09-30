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
        source,
        YEAR(played_at) AS played_at_year,
        CASE WHEN ms_played >= 290754 THEN 'A' WHEN ms_played >= 112786 THEN 'B' ELSE 'C' END AS ms_played_tier,
        DATE_FORMAT(played_at, 'yyyy-MM-dd HH:00') AS played_at_label
    FROM plays
    WHERE is_skipped = TRUE
),
with_details_track AS (
    -- Join tracks
    SELECT
        s.*,
        t.moods AS track_moods,
        t.is_explicit AS track_is_explicit,
        t.duration_sec AS track_duration_sec
    FROM base s
    LEFT JOIN tracks t ON t.id = s.track_id
)
SELECT id, metric_name, reading
FROM (
    SELECT id, track_duration_sec, played_at_year
    FROM with_details_track
) src
UNPIVOT (
    reading FOR metric_name IN (track_duration_sec, played_at_year)
) unp
ORDER BY id, metric_name
