WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        source,
        LENGTH(TRIM(source)) AS source_len,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket,
        TO_DATE(played_at) AS played_at_date
    FROM plays
    WHERE ms_played <= 241585
      AND MONTH(played_at) IN (1, 2)
),
listener_id_stats AS (
    -- Median ms_played per listener_id
    SELECT listener_id, PERCENTILE_APPROX(ms_played, 0.5) AS median_ms_played
    FROM prepared
    GROUP BY listener_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_ms_played,
        ROUND(s.ms_played - g.median_ms_played, 2) AS ms_played_minus_median
    FROM prepared s
    JOIN listener_id_stats g ON g.listener_id = s.listener_id
    WHERE s.ms_played >= g.median_ms_played
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(source_len - AVG(source_len) OVER (PARTITION BY source), 2) AS source_len_vs_source_avg,
        NTH_VALUE(ms_played, 2) OVER (PARTITION BY source, listener_id ORDER BY ms_played_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played,
        DENSE_RANK() OVER (PARTITION BY source, listener_id ORDER BY ms_played_bucket, id) AS ms_played_bucket_dense_rank,
        SUM(ms_played_minus_median) OVER (PARTITION BY source, listener_id ORDER BY ms_played_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ms_played_minus_median,
        SUM(ms_played) OVER (PARTITION BY source, listener_id ORDER BY ms_played_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ms_played
    FROM outliers
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM with_windows
    WHERE running_ms_played > (SELECT AVG(running_ms_played) FROM with_windows)
),
detail_rows AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.source,
        s.played_at_date,
        s.median_ms_played,
        s.ms_played_minus_median,
        s.source_len_vs_source_avg,
        s.second_ms_played,
        s.ms_played_bucket_dense_rank,
        s.running_ms_played,
        p.id AS play_id,
        p.played_at AS play_played_at,
        p.device AS play_device
    FROM filtered s
    JOIN plays p ON p.listener_id = s.listener_id AND HOUR(p.played_at) BETWEEN 6 AND 12
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, play_id, metric_name, reading
FROM (
    SELECT id, play_id, median_ms_played, ms_played_minus_median, source_len_vs_source_avg
    FROM detail_rows
) src
UNPIVOT INCLUDE NULLS (
    reading FOR metric_name IN (median_ms_played AS m_median_ms_played, ms_played_minus_median AS m_ms_played_minus_median, source_len_vs_source_avg AS source_len_vs_source_avg_v)
) unp
