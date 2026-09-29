WITH filtered_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        DATE_TRUNC('YEAR', played_at) AS played_at_year,
        COALESCE(source, 'UNKNOWN') AS source_or_unknown
    FROM plays
),
screened AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM filtered_plays s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND p.device = 'desktop'
        )
),
track_id_stats AS (
    -- Median ms_played per track_id
    SELECT track_id, PERCENTILE_APPROX(ms_played, 0.5) AS median_ms_played
    FROM screened
    GROUP BY track_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_ms_played,
        ROUND(s.ms_played - g.median_ms_played, 2) AS ms_played_minus_median
    FROM screened s
    JOIN track_id_stats g ON g.track_id = s.track_id
    WHERE s.ms_played >= g.median_ms_played
),
listener_id_stats AS (
    -- Avg ms_played per listener_id
    SELECT listener_id, AVG(ms_played) AS avg_ms_played
    FROM above_average
    GROUP BY listener_id
),
above_average_2 AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_ms_played,
        ROUND(s.ms_played - g.avg_ms_played, 2) AS ms_played_minus_avg
    FROM above_average s
    JOIN listener_id_stats g ON g.listener_id = s.listener_id
    WHERE s.ms_played > g.avg_ms_played
),
totals AS (
    -- Group-level totals
    SELECT
        device,
        source_or_unknown,
        COUNT(*) AS cnt,
        MIN(median_ms_played) AS min_median_ms_played,
        ROUND(AVG(ms_played_minus_avg), 2) AS avg_ms_played_minus_avg,
        ROUND(STDDEV(ms_played_minus_median), 2) AS stddev_ms_played_minus_median,
        SUM(median_ms_played) FILTER (WHERE NOT is_skipped) AS median_ms_played_not_is_skipped
    FROM above_average_2
    GROUP BY device, source_or_unknown
    HAVING SUM(median_ms_played) > 44500.0
),
with_activity AS (
    -- Compare against the overall min
    SELECT *
    FROM totals
    WHERE avg_ms_played_minus_avg > (SELECT MIN(avg_ms_played_minus_avg) FROM totals)
)
-- Final projection
SELECT
    device,
    source_or_unknown,
    cnt,
    avg_ms_played_minus_avg,
    stddev_ms_played_minus_median,
    median_ms_played_not_is_skipped,
    min_median_ms_played + avg_ms_played_minus_avg AS min_median_ms_played_plus_avg_ms_played_minus_av
FROM with_activity
WHERE min_median_ms_played BETWEEN 44500.0 AND 261750.0
ORDER BY min_median_ms_played_plus_avg_ms_played_minus_av DESC
