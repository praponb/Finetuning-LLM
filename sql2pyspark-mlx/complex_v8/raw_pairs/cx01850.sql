WITH valid_plays AS (
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
        CONCAT(source, '#', CAST(listener_id AS STRING)) AS source_ref
    FROM plays
    WHERE TO_DATE(played_at) < DATE '2025-02-24'
      AND device IS NOT NULL
),
listener_id_stats AS (
    -- Avg ms_played per listener_id
    SELECT listener_id, AVG(ms_played) AS avg_ms_played
    FROM valid_plays
    GROUP BY listener_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_ms_played,
        ROUND(s.ms_played - g.avg_ms_played, 2) AS ms_played_minus_avg
    FROM valid_plays s
    JOIN listener_id_stats g ON g.listener_id = s.listener_id
    WHERE s.ms_played > g.avg_ms_played
)
SELECT id, metric_name, value
FROM (
    SELECT id, avg_ms_played, ms_played_minus_avg
    FROM compared
) src
UNPIVOT (
    value FOR metric_name IN (avg_ms_played, ms_played_minus_avg)
) unp
ORDER BY id, metric_name
