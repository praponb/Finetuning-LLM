WITH scoped_plays AS (
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
        LOWER(REGEXP_REPLACE(TRIM(device), ' ', '_')) AS device_slug,
        LENGTH(TRIM(source)) AS source_len,
        MONTH(played_at) AS played_at_month
    FROM plays
    WHERE played_at < TIMESTAMP '2025-02-18 12:00:00'
),
screened AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM scoped_plays s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND p.source IS NOT NULL
              AND p.is_skipped = TRUE
        )
),
with_details_track AS (
    -- Attach track attributes
    SELECT s.*, t.release_date AS track_release_date
    FROM screened s
    JOIN tracks t ON t.id = s.track_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT track_release_date, track_id, device, ms_played
    FROM with_details_track
) src
PIVOT (
    SUM(ms_played) AS total, MAX(ms_played) AS max_v
    FOR device IN ('car', 'desktop', 'phone', 'speaker')
)
ORDER BY track_release_date, track_id
