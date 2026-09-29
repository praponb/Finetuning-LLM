WITH valid_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        source
    FROM plays
    WHERE played_at >= TIMESTAMP '2025-01-31 10:00:00'
),
enriched_listener AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.source,
        l.username AS listener_username
    FROM valid_plays s
    JOIN listeners l ON l.id = s.listener_id
)
SELECT
    listener_id,
    track_id,
    COALESCE(source, 'ALL') AS source,
    COUNT(*) AS row_count,
    COUNT(DISTINCT source) AS distinct_sources
FROM enriched_listener
GROUP BY GROUPING SETS ((listener_id, track_id, source), (listener_id))
ORDER BY listener_id NULLS LAST, track_id NULLS LAST, source NULLS LAST
