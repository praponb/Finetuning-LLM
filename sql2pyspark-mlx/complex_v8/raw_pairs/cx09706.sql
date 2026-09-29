WITH valid_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        is_skipped,
        source,
        CASE WHEN is_skipped THEN 1 ELSE 0 END AS is_skipped_flag
    FROM plays
    WHERE played_at < TIMESTAMP '2025-02-18 12:00:00'
),
source_stats AS (
    -- Max ms_played per source
    SELECT source, MAX(ms_played) AS max_ms_played
    FROM valid_plays
    GROUP BY source
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_ms_played,
        ROUND(s.ms_played - g.max_ms_played, 2) AS ms_played_minus_max
    FROM valid_plays s
    JOIN source_stats g ON g.source = s.source
    WHERE s.ms_played = g.max_ms_played
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, reading
FROM (
    SELECT id, ms_played_minus_max, max_ms_played
    FROM outliers
) src
UNPIVOT (
    reading FOR metric_name IN (ms_played_minus_max, max_ms_played)
) unp
ORDER BY id, metric_name
