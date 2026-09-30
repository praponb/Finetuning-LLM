WITH filtered AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source
    FROM plays
    WHERE NOT is_skipped
),
combined_details_listener AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        l.username AS listener_username
    FROM filtered s
    JOIN listeners l ON l.id = s.listener_id
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        listener_username,
        LOWER(REGEXP_REPLACE(TRIM(listener_username), ' ', '_')) AS listener_username_slug
    FROM combined_details_listener
)
-- Final projection
SELECT
    id,
    listener_username,
    listener_username_slug,
    CONCAT(listener_username_slug, '#', CAST(id AS STRING)) AS listener_username_slug_ref
FROM enriched
WHERE UPPER(TRIM(listener_username)) LIKE 'G%'
ORDER BY listener_username_slug_ref NULLS LAST, listener_username_slug
