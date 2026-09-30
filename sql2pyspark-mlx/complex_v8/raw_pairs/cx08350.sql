WITH valid_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM plays
    WHERE source <> 'album'
),
screened AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM valid_plays s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.track_id
              AND p.is_skipped = FALSE
              AND p.listener_id <= 19
        )
),
with_details_track AS (
    -- Attach track attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.device,
        s.is_skipped,
        s.source,
        t.duration_sec AS track_duration_sec,
        t.artist_id AS track_artist_id,
        t.title AS track_title
    FROM screened s
    LEFT JOIN tracks t ON t.id = s.track_id
),
with_activity AS (
    -- Compare against the overall max
    SELECT *
    FROM with_details_track
    WHERE track_duration_sec < (SELECT MAX(track_duration_sec) FROM with_details_track)
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        source,
        track_duration_sec,
        ROUND(AVG(ms_played) OVER (PARTITION BY track_id, listener_id), 2) AS track_id_listener_id_avg_ms_played,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY track_id, listener_id), 2) AS ms_played_vs_track_id_listener_id_avg
    FROM with_activity
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        LOWER(TRIM(device)) AS device_clean,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket
    FROM with_windows
)
SELECT
    COALESCE(device, 'ALL') AS device,
    COALESCE(source, 'ALL') AS source,
    COUNT(DISTINCT device) AS distinct_devices
FROM enriched
GROUP BY GROUPING SETS ((device, source), (device), ())
ORDER BY device NULLS LAST, source NULLS LAST
