WITH base_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        CASE WHEN ms_played >= 223828 THEN 'large' WHEN ms_played >= 176302 THEN 'medium' ELSE 'small' END AS ms_played_tier
    FROM plays
    WHERE listener_id >= 9
      AND source IN ('radio', 'album')
),
source_stats AS (
    -- Avg ms_played per source
    SELECT source, AVG(ms_played) AS avg_ms_played
    FROM base_plays
    GROUP BY source
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_ms_played,
        ROUND(s.ms_played - g.avg_ms_played, 2) AS ms_played_minus_avg
    FROM base_plays s
    JOIN source_stats g ON g.source = s.source
    WHERE s.ms_played > g.avg_ms_played
),
qualified AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM vs_group s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.track_id
              AND p.is_skipped = FALSE
              AND p.listener_id >= 2
        )
)
-- Subtotals with GROUPING SETS
SELECT
    is_skipped,
    source,
    track_id,
    GROUPING(is_skipped) AS is_total_is_skipped,
    COUNT(*) AS row_count,
    MAX(ms_played_minus_avg) AS max_ms_played_minus_avg
FROM qualified
GROUP BY GROUPING SETS ((is_skipped, source, track_id), (is_skipped), (source), ())
ORDER BY is_skipped NULLS LAST, source NULLS LAST, track_id NULLS LAST
