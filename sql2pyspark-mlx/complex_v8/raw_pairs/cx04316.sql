WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source
    FROM plays
    WHERE source = 'album'
),
with_activity AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND p.ms_played < 326250
              AND NOT p.is_skipped
        )
),
qualified AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM with_activity s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND p.device IN ('speaker', 'desktop')
        )
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        track_id,
        device,
        CONCAT_WS('-', device, source) AS device_source_label,
        SUBSTRING(TRIM(device), 1, 2) AS device_prefix,
        DATEDIFF(DATE '2025-06-30', played_at) AS days_since_played_at
    FROM qualified
)
-- Final projection
SELECT id, device_prefix, days_since_played_at
FROM scored
ORDER BY id DESC NULLS FIRST, days_since_played_at
