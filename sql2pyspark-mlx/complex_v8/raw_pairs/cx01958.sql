WITH filtered_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        device,
        is_skipped,
        source,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket
    FROM plays
    WHERE ms_played >= 129336
),
source_stats AS (
    -- Median ms_played_bucket per source
    SELECT source, PERCENTILE_APPROX(ms_played_bucket, 0.5) AS median_ms_played_bucket
    FROM filtered_plays
    GROUP BY source
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_ms_played_bucket,
        ROUND(s.ms_played_bucket - g.median_ms_played_bucket, 2) AS ms_played_bucket_minus_median
    FROM filtered_plays s
    JOIN source_stats g ON g.source = s.source
    WHERE s.ms_played_bucket >= g.median_ms_played_bucket
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTILE(4) OVER w_track_id_by_median_ms_played AS median_ms_played_bucket_quartile,
        ROUND(ms_played_bucket - AVG(ms_played_bucket) OVER (PARTITION BY device, source), 2) AS ms_played_bucket_vs_device_source_avg,
        LAST_VALUE(median_ms_played_bucket) OVER (PARTITION BY track_id ORDER BY median_ms_played_bucket ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_median_ms_played_bucket,
        ms_played_bucket - LAG(ms_played_bucket) OVER (PARTITION BY track_id ORDER BY median_ms_played_bucket ASC, id) AS ms_played_bucket_change
    FROM vs_group
    WINDOW w_track_id_by_median_ms_played AS (PARTITION BY track_id ORDER BY median_ms_played_bucket ASC, id)
),
track_id_stats AS (
    -- Median ms_played_bucket per track_id
    SELECT track_id, PERCENTILE_APPROX(ms_played_bucket, 0.5) AS median_ms_played_bucket_2
    FROM ranked
    GROUP BY track_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_ms_played_bucket_2,
        ROUND(s.ms_played_bucket - g.median_ms_played_bucket_2, 2) AS ms_played_bucket_minus_median_2
    FROM ranked s
    JOIN track_id_stats g ON g.track_id = s.track_id
    WHERE s.ms_played_bucket >= g.median_ms_played_bucket_2
),
without_matches AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM outliers s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND NOT p.is_skipped
        )
),
combined_details_track AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.source,
        s.median_ms_played_bucket_quartile,
        s.ms_played_bucket_vs_device_source_avg,
        s.ms_played_bucket_change,
        s.ms_played_bucket_minus_median_2,
        t.duration_sec AS track_duration_sec,
        t.release_date AS track_release_date,
        t.artist_id AS track_artist_id
    FROM without_matches s
    JOIN tracks t ON t.id = s.track_id AND ARRAY_CONTAINS(t.moods, 'party')
    WHERE t.duration_sec BETWEEN 144 AND 353
)
-- Final projection
SELECT
    id,
    listener_id,
    device,
    source,
    ms_played_bucket_vs_device_source_avg,
    ms_played_bucket_change,
    ms_played_bucket_minus_median_2,
    track_duration_sec,
    track_release_date,
    track_artist_id,
    CASE WHEN ms_played_bucket_change >= 80000 THEN 'HIGH' WHEN ms_played_bucket_change >= -240000 THEN 'MEDIUM' ELSE 'LOW' END AS ms_played_bucket_change_tier
FROM combined_details_track
WHERE ms_played_bucket_minus_median_2 <= 240000.0
ORDER BY listener_id
