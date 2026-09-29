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
        source
    FROM plays
    WHERE played_at >= TIMESTAMP '2025-01-31 12:00:00'
),
with_details_listener AS (
    -- Attach listener attributes
    SELECT s.*, l.country AS listener_country
    FROM valid_plays s
    JOIN listeners l ON l.id = s.listener_id
),
listener_id_stats AS (
    -- Avg ms_played per listener_id
    SELECT listener_id, AVG(ms_played) AS avg_ms_played
    FROM with_details_listener
    GROUP BY listener_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_ms_played,
        ROUND(s.ms_played - g.avg_ms_played, 2) AS ms_played_minus_avg
    FROM with_details_listener s
    JOIN listener_id_stats g ON g.listener_id = s.listener_id
    WHERE s.ms_played > g.avg_ms_played
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(ms_played_minus_avg) OVER (ORDER BY ms_played DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS ms_played_minus_avg_avg_4,
        FIRST_VALUE(avg_ms_played) IGNORE NULLS OVER (PARTITION BY device, listener_country ORDER BY ms_played_minus_avg DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_avg_ms_played,
        LEAD(played_at, 3) OVER (PARTITION BY device, listener_country ORDER BY ms_played_minus_avg DESC, id) AS next3_played_at,
        MAX(ms_played) OVER (PARTITION BY device, listener_country ORDER BY ms_played_minus_avg DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_ms_played_so_far,
        MIN(ms_played_minus_avg) OVER (ORDER BY ms_played DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_ms_played_minus_avg_so_far
    FROM vs_group
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, amount
FROM (
    SELECT id, ms_played, max_ms_played_so_far
    FROM metrics
) src
UNPIVOT INCLUDE NULLS (
    amount FOR metric IN (ms_played, max_ms_played_so_far)
) unp
ORDER BY id, metric
