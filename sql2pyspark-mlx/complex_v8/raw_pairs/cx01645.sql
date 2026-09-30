WITH base AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        source,
        UPPER(TRIM(source)) AS source_clean,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket
    FROM plays
    WHERE is_skipped = TRUE
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.source,
        s.ms_played_bucket,
        t.artist_id AS track_artist_id
    FROM base s
    JOIN tracks t ON t.id = s.track_id
),
track_id_stats AS (
    -- Max ms_played_bucket per track_id
    SELECT track_id, MAX(ms_played_bucket) AS max_ms_played_bucket
    FROM enriched
    GROUP BY track_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_ms_played_bucket,
        ROUND(s.ms_played_bucket - g.max_ms_played_bucket, 2) AS ms_played_bucket_minus_max
    FROM enriched s
    JOIN track_id_stats g ON g.track_id = s.track_id
    WHERE s.ms_played_bucket = g.max_ms_played_bucket
),
without_matches AS (
    -- Keep rows whose listener matches
    SELECT s.*
    FROM compared s
    WHERE EXISTS (
            SELECT 1
            FROM listeners l
            WHERE l.id = s.listener_id
              AND UPPER(TRIM(l.username)) LIKE 'E%'
        )
),
totals AS (
    -- Summarise per group
    SELECT
        CASE WHEN max_ms_played_bucket >= 321000.0 THEN 'large' WHEN max_ms_played_bucket >= 143250.0 THEN 'medium' ELSE 'small' END AS max_ms_played_bucket_tier,
        ROUND(AVG(ms_played_bucket_minus_max), 2) AS avg_ms_played_bucket_minus_max,
        ROUND(AVG(max_ms_played_bucket), 2) AS avg_max_ms_played_bucket,
        SUM(CASE WHEN source IN ('radio', 'playlist', 'search') THEN 1 ELSE 0 END) AS n_source_in_radio_playlist_sea,
        ARRAY_SORT(COLLECT_SET(source)) AS source_list,
        SUM(max_ms_played_bucket) AS total_max_ms_played_bucket
    FROM without_matches
    GROUP BY CASE WHEN max_ms_played_bucket >= 321000.0 THEN 'large' WHEN max_ms_played_bucket >= 143250.0 THEN 'medium' ELSE 'small' END
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM totals
    WHERE avg_ms_played_bucket_minus_max > (SELECT AVG(avg_ms_played_bucket_minus_max) FROM totals)
)
-- Final projection
SELECT DISTINCT total_max_ms_played_bucket, n_source_in_radio_playlist_sea, source_list
FROM screened
WHERE max_ms_played_bucket_tier IS NOT NULL
ORDER BY total_max_ms_played_bucket
