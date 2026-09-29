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
        CONCAT_WS(': ', source, device) AS source_device_label,
        SUBSTRING(TRIM(device), 1, 2) AS device_prefix
    FROM plays
    WHERE (source = 'album' OR listener_id IS NOT NULL)
),
combined_details AS (
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
        s.source_device_label,
        s.device_prefix,
        t.duration_sec AS track_duration_sec,
        t.title AS track_title
    FROM recent_plays s
    JOIN tracks t ON t.id = s.track_id
),
qualified AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM combined_details s
    WHERE s.track_id IN (
            SELECT p.track_id
            FROM plays p
            WHERE p.ms_played <= 318978
              AND p.listener_id <= 17
        )
      AND s.device_prefix IS NOT NULL
)
SELECT
    COALESCE(source, 'ALL') AS source,
    COALESCE(device, 'ALL') AS device,
    ROUND(AVG(ms_played), 2) AS avg_ms_played
FROM qualified
GROUP BY CUBE(source, device)
