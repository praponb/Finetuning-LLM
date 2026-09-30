WITH recent AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        CONCAT_WS('-', source, device) AS source_device_label,
        DATEDIFF(DATE '2025-12-31', played_at) AS days_since_played_at
    FROM plays
),
filtered AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM recent s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND p.ms_played BETWEEN 56858 AND 333795
              AND p.source IS NOT NULL
        )
)
-- Subtotals with CUBE
SELECT
    is_skipped,
    track_id,
    MAX(days_since_played_at) AS max_days_since_played_at,
    MIN(days_since_played_at) AS min_days_since_played_at
FROM filtered
GROUP BY CUBE(is_skipped, track_id)
