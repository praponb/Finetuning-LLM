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
        LOWER(TRIM(source)) AS source_clean,
        INITCAP(TRIM(source)) AS source_clean_2,
        CONCAT_WS(': ', device, source) AS device_source_label
    FROM plays
    WHERE device IN ('speaker', 'phone')
),
screened AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM scoped_plays s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND p.device NOT IN ('speaker', 'desktop')
        )
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        listener_id,
        played_at,
        device,
        is_skipped,
        source_clean_2,
        device_source_label,
        LOWER(REGEXP_REPLACE(TRIM(device), ' ', '_')) AS device_slug,
        DATE_TRUNC('HOUR', played_at) AS played_at_hour
    FROM screened
),
joined_listener AS (
    -- Attach listener attributes
    SELECT s.*, l.country AS listener_country
    FROM enriched s
    JOIN listeners l ON l.id = s.listener_id
)
-- Final projection
SELECT
    id,
    played_at,
    device,
    is_skipped,
    source_clean_2,
    device_source_label,
    played_at_hour,
    CONCAT(listener_country, '#', CAST(id AS STRING)) AS listener_country_ref
FROM joined_listener
WHERE TO_DATE(played_at) < DATE '2025-02-24'
ORDER BY played_at DESC, source_clean_2 DESC
