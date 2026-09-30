WITH scoped_plays AS (
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
        SUBSTRING(TRIM(device), 1, 4) AS device_prefix,
        CASE WHEN source IN ('search', 'playlist') THEN 'group_a' WHEN source = 'radio' THEN 'group_b' ELSE 'other' END AS source_group
    FROM plays
    WHERE device = 'phone'
      AND played_at >= TIMESTAMP '2025-01-31 12:00:00'
),
exploded_detail_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.device,
        s.is_skipped,
        s.source,
        s.device_prefix,
        p.id AS play_id,
        p.played_at AS play_played_at,
        p.listener_id AS play_listener_id,
        p.device AS play_device
    FROM scoped_plays s
    LEFT JOIN plays p ON p.track_id = s.track_id AND p.listener_id <= 16
),
source_stats AS (
    -- Median ms_played per source
    SELECT source, PERCENTILE_APPROX(ms_played, 0.5) AS median_ms_played
    FROM exploded_detail_plays
    GROUP BY source
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_ms_played,
        ROUND(s.ms_played - g.median_ms_played, 2) AS ms_played_minus_median
    FROM exploded_detail_plays s
    JOIN source_stats g ON g.source = s.source
    WHERE s.ms_played >= g.median_ms_played
),
grouped AS (
    -- Summarise per group
    SELECT
        device,
        DATE_TRUNC('MONTH', play_played_at) AS play_played_at_month,
        MIN(ms_played) AS min_ms_played,
        SUM(CASE WHEN source IN ('playlist', 'search') THEN 1 ELSE 0 END) AS n_source_in_playlist_search,
        ARRAY_SORT(COLLECT_SET(play_device)) AS play_device_list,
        SUM(ms_played) AS total_ms_played,
        ROUND(SUM(ms_played) / NULLIF(SUM(median_ms_played), 0), 3) AS ms_played_to_median_ms_played_ratio
    FROM compared
    GROUP BY device, DATE_TRUNC('MONTH', play_played_at)
    HAVING MAX(ms_played_minus_median) > -160000.0
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        FLOOR(min_ms_played / 50000) * 50000 AS min_ms_played_bucket,
        SUBSTRING(TRIM(device), 1, 3) AS device_prefix,
        DAYOFWEEK(play_played_at_month) AS play_played_at_month_dayofweek
    FROM grouped
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM tagged
    WHERE min_ms_played > (SELECT AVG(min_ms_played) FROM tagged)
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY play_played_at_month_dayofweek, n_source_in_playlist_search) AS rows_in_play_played_at_month_dayofweek_n_source_,
        FIRST_VALUE(n_source_in_playlist_search) OVER (PARTITION BY play_played_at_month_dayofweek, n_source_in_playlist_search ORDER BY play_played_at_month_dayofweek DESC, device, play_played_at_month ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_n_source_in_playlist_search
    FROM with_activity
)
-- Final projection
SELECT
    device,
    play_played_at_month,
    min_ms_played,
    n_source_in_playlist_search,
    total_ms_played,
    ms_played_to_median_ms_played_ratio,
    min_ms_played_bucket,
    device_prefix,
    play_played_at_month_dayofweek,
    rows_in_play_played_at_month_dayofweek_n_source_,
    first_n_source_in_playlist_search,
    COALESCE(min_ms_played_bucket, 0) AS min_ms_played_bucket_filled
FROM with_windows
ORDER BY play_played_at_month DESC, min_ms_played_bucket_filled, device
LIMIT 5
