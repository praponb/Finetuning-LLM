WITH recent AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        source
    FROM plays
    WHERE played_at < TIMESTAMP '2025-03-02 10:00:00'
      AND is_skipped = FALSE
),
joined AS (
    -- Attach track attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device,
        s.source,
        t.release_date AS track_release_date
    FROM recent s
    JOIN tracks t ON t.id = s.track_id
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        listener_id,
        track_id,
        source,
        track_release_date,
        COALESCE(device, 'UNKNOWN') AS device_or_unknown
    FROM joined
    WHERE listener_id <= 4
),
screened AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND TO_DATE(p.played_at) < DATE '2025-02-18'
        )
      AND s.source IN ('radio', 'album')
)
-- Final projection
SELECT
    id,
    source,
    track_release_date,
    DATEDIFF(DATE '2026-01-01', track_release_date) AS days_since_track_release_date
FROM screened
WHERE listener_id IS NOT NULL
ORDER BY source DESC NULLS FIRST, id
LIMIT 10
