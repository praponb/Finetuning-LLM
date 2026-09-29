WITH cleaned_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        DATE_FORMAT(played_at, 'yyyy-MM') AS played_at_label,
        SUBSTRING(TRIM(device), 1, 2) AS device_prefix,
        DATE_FORMAT(played_at, 'yyyy-MM') AS played_at_label_2
    FROM plays
    WHERE NOT is_skipped
),
screened AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM cleaned_plays s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.track_id
              AND HOUR(p.played_at) BETWEEN 8 AND 11
              AND p.source = 'playlist'
        )
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        LENGTH(TRIM(device)) AS device_len,
        CASE WHEN ms_played >= 220498 THEN 'A' WHEN ms_played >= 95960 THEN 'B' ELSE 'C' END AS ms_played_tier
    FROM screened
    WHERE device IS NOT NULL
),
qualified AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM scored s
    WHERE s.track_id IN (
            SELECT p.track_id
            FROM plays p
            WHERE p.device NOT IN ('speaker', 'tv')
              AND p.listener_id IS NOT NULL
        )
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    ms_played,
    source,
    played_at_label,
    played_at_label_2,
    ms_played_tier,
    LOWER(TRIM(device)) AS device_clean,
    CASE WHEN ms_played >= 241585 THEN 'A' WHEN ms_played >= 176302 THEN 'B' ELSE 'C' END AS ms_played_tier_2
FROM qualified
ORDER BY listener_id
