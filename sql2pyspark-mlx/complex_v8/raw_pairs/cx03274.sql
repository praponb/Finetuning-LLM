WITH recent AS (
    -- Scope the plays rows
    SELECT *
    FROM plays
    WHERE ms_played > 73177
),
device_stats AS (
    -- Avg ms_played per device
    SELECT device, AVG(ms_played) AS avg_ms_played
    FROM recent
    GROUP BY device
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_ms_played,
        ROUND(s.ms_played - g.avg_ms_played, 2) AS ms_played_minus_avg
    FROM recent s
    JOIN device_stats g ON g.device = s.device
    WHERE s.ms_played > g.avg_ms_played
),
source_stats AS (
    -- Avg ms_played per source
    SELECT source, AVG(ms_played) AS avg_ms_played_2
    FROM vs_group
    GROUP BY source
),
vs_group_2 AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_ms_played_2,
        ROUND(s.ms_played - g.avg_ms_played_2, 2) AS ms_played_minus_avg_2
    FROM vs_group s
    JOIN source_stats g ON g.source = s.source
    WHERE s.ms_played > g.avg_ms_played_2
),
screened AS (
    -- Keep rows whose listener matches
    SELECT s.*
    FROM vs_group_2 s
    WHERE NOT EXISTS (
            SELECT 1
            FROM listeners l
            WHERE l.id = s.listener_id
              AND l.birth_year BETWEEN 1968 AND 1990
        )
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    device,
    is_skipped,
    source,
    ms_played_minus_avg,
    ms_played_minus_avg_2,
    ABS(avg_ms_played_2 - 222250.0) AS avg_ms_played_2_distance,
    LOWER(REGEXP_REPLACE(TRIM(source), ' ', '_')) AS source_slug
FROM screened
ORDER BY avg_ms_played_2_distance DESC
