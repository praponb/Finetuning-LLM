WITH recent AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        source,
        INITCAP(TRIM(device)) AS device_clean,
        INITCAP(TRIM(source)) AS source_clean
    FROM plays
    WHERE is_skipped = FALSE
),
without_matches AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM recent s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND p.device IS NOT NULL
        )
      AND TO_DATE(s.played_at) < DATE '2025-02-06'
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        DATE_FORMAT(played_at, 'yyyy-MM-dd') AS played_at_label,
        COALESCE(source, 'UNKNOWN') AS source_or_unknown
    FROM without_matches
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    device,
    device_clean,
    played_at_label,
    COALESCE(source, 'UNKNOWN') AS source_or_unknown_2
FROM enriched
WHERE track_id IS NOT NULL
ORDER BY source_or_unknown_2
