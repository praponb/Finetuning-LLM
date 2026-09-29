WITH valid AS (
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
        UPPER(TRIM(source)) AS source_clean
    FROM plays
    WHERE (source = 'album' OR is_skipped = FALSE)
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.device,
        s.source,
        s.source_clean,
        t.is_explicit AS track_is_explicit
    FROM valid s
    JOIN tracks t ON t.id = s.track_id
    WHERE t.is_explicit = TRUE
),
filtered AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM joined s
    WHERE s.track_id IN (
            SELECT p.track_id
            FROM plays p
            WHERE p.source IN ('radio', 'search', 'album')
              AND p.listener_id >= 5
        )
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    ms_played,
    device,
    source,
    track_is_explicit,
    played_at + INTERVAL 30 MINUTES AS played_at_plus_30m,
    LOWER(REGEXP_REPLACE(TRIM(source), ' ', '_')) AS source_slug
FROM filtered
ORDER BY played_at NULLS LAST, id
LIMIT 10
