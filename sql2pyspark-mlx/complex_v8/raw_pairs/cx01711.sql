WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        is_skipped,
        source,
        SUBSTRING(TRIM(source), 1, 2) AS source_prefix,
        SUBSTRING(TRIM(source), 1, 2) AS source_prefix_2,
        LENGTH(TRIM(source)) AS source_len
    FROM plays
    WHERE ms_played <= 218129
      AND played_at < TIMESTAMP '2025-03-08 10:00:00'
),
with_parent AS (
    -- Attach track attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.is_skipped,
        s.source,
        s.source_prefix,
        s.source_prefix_2,
        s.source_len,
        t.title AS track_title,
        t.duration_sec AS track_duration_sec
    FROM scoped s
    JOIN tracks t ON t.id = s.track_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(track_duration_sec - AVG(track_duration_sec) OVER w_is_skipped_source_len, 2) AS track_duration_sec_vs_is_skipped_source_len_avg,
        track_duration_sec - LAG(track_duration_sec) OVER (PARTITION BY track_id, is_skipped ORDER BY track_duration_sec, id) AS track_duration_sec_change,
        NTILE(5) OVER w_track_id_is_skipped_by_track AS track_duration_sec_quintile,
        NTH_VALUE(source_len, 2) OVER (PARTITION BY is_skipped ORDER BY track_duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_source_len,
        LAG(track_duration_sec) OVER (PARTITION BY is_skipped ORDER BY track_duration_sec, id) AS prev_track_duration_sec
    FROM with_parent
    WINDOW w_is_skipped_source_len AS (PARTITION BY is_skipped, source_len), w_track_id_is_skipped_by_track AS (PARTITION BY track_id, is_skipped ORDER BY track_duration_sec, id)
),
source_stats AS (
    -- Avg track_duration_sec_quintile per source
    SELECT source, AVG(track_duration_sec_quintile) AS avg_track_duration_sec_quintile
    FROM metrics
    GROUP BY source
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_track_duration_sec_quintile,
        ROUND(s.track_duration_sec_quintile - g.avg_track_duration_sec_quintile, 2) AS track_duration_sec_quintile_minus_avg
    FROM metrics s
    JOIN source_stats g ON g.source = s.source
    WHERE s.track_duration_sec_quintile > g.avg_track_duration_sec_quintile
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT track_id, source_len, source, avg_track_duration_sec_quintile
    FROM above_average
) src
PIVOT (
    MIN(avg_track_duration_sec_quintile)
    FOR source IN ('radio', 'search')
)
ORDER BY track_id, source_len
