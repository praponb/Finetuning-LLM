WITH recent_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        LOWER(REGEXP_REPLACE(TRIM(source), ' ', '_')) AS source_slug
    FROM plays
    WHERE listener_id >= 2
      AND source = 'album'
)
SELECT id AS entity_id
FROM recent_plays
EXCEPT ALL
SELECT track_id AS entity_id
FROM plays
WHERE device = 'phone'
ORDER BY entity_id
