WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        is_skipped,
        source,
        LOWER(REGEXP_REPLACE(TRIM(source), ' ', '_')) AS source_slug
    FROM plays
    WHERE track_id IS NOT NULL
),
track_id_stats AS (
    -- Max ms_played per track_id
    SELECT track_id, MAX(ms_played) AS max_ms_played
    FROM cleaned
    GROUP BY track_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_ms_played,
        ROUND(s.ms_played - g.max_ms_played, 2) AS ms_played_minus_max
    FROM cleaned s
    JOIN track_id_stats g ON g.track_id = s.track_id
    WHERE s.ms_played = g.max_ms_played
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        track_id,
        SUM(ms_played) AS total_ms_played,
        COUNT(*) AS row_count,
        ROUND(AVG(max_ms_played), 2) AS avg_max_ms_played,
        MAX(max_ms_played) AS max_max_ms_played,
        COUNT(DISTINCT listener_id) AS distinct_listener_ids
    FROM vs_group
    GROUP BY track_id
)
-- Final projection
SELECT
    track_id,
    total_ms_played,
    row_count,
    avg_max_ms_played,
    max_max_ms_played,
    distinct_listener_ids
FROM rollup_base
ORDER BY distinct_listener_ids, total_ms_played DESC
