WITH valid AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        source,
        LENGTH(TRIM(device)) AS device_len,
        CASE WHEN ms_played >= 291723 THEN 'HIGH' WHEN ms_played >= 145720 THEN 'MEDIUM' ELSE 'LOW' END AS ms_played_tier,
        COALESCE(source, 'UNKNOWN') AS source_or_unknown
    FROM plays
    WHERE NOT is_skipped
),
enriched AS (
    -- Attach listener attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.device,
        s.device_len,
        s.ms_played_tier,
        l.country AS listener_country,
        t.is_explicit AS track_is_explicit
    FROM valid s
    JOIN listeners l ON l.id = s.listener_id
    LEFT JOIN tracks t ON t.id = s.track_id
),
filtered AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM enriched s
    WHERE s.track_id IN (
            SELECT p.track_id
            FROM plays p
            WHERE p.device <> 'desktop'
        )
      AND s.track_is_explicit = TRUE
)
-- Final projection
SELECT
    id,
    ms_played,
    device,
    ms_played_tier,
    listener_country,
    track_is_explicit,
    LOWER(REGEXP_REPLACE(TRIM(listener_country), ' ', '_')) AS listener_country_slug,
    LOWER(REGEXP_REPLACE(TRIM(listener_country), ' ', '_')) AS listener_country_slug_2
FROM filtered
WHERE device_len > 7
ORDER BY listener_country_slug_2 DESC, device, id
LIMIT 5
