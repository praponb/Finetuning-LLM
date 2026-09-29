WITH filtered_plays AS (
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
        ABS(ms_played - 166517) AS ms_played_distance,
        DATEDIFF(DATE '2025-12-31', played_at) AS days_since_played_at,
        ABS(ms_played - 107912) AS ms_played_distance_2
    FROM plays
    WHERE device IN ('desktop', 'phone', 'tv')
      AND listener_id <= 4
),
screened AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM filtered_plays s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND p.ms_played <= 246779
        )
      AND s.listener_id <= 10
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(ms_played_distance_2) OVER (PARTITION BY is_skipped, listener_id) AS is_skipped_listener_id_max_ms_played_distance_2,
        ROUND(PERCENT_RANK() OVER (PARTITION BY track_id, MONTH(played_at) ORDER BY played_at NULLS LAST), 3) AS played_at_percent_rank
    FROM screened
),
device_stats AS (
    -- Median days_since_played_at per device
    SELECT device, PERCENTILE_APPROX(days_since_played_at, 0.5) AS median_days_since_played_at
    FROM sequenced
    GROUP BY device
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_days_since_played_at,
        ROUND(s.days_since_played_at - g.median_days_since_played_at, 2) AS days_since_played_at_minus_median
    FROM sequenced s
    JOIN device_stats g ON g.device = s.device
    WHERE s.days_since_played_at >= g.median_days_since_played_at
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_all_by_played_at_percent_ran, 3) AS played_at_percent_rank_percent_rank,
        ROUND(ms_played_distance - AVG(ms_played_distance) OVER (PARTITION BY track_id), 2) AS ms_played_distance_vs_track_id_avg,
        DENSE_RANK() OVER (ORDER BY played_at_percent_rank DESC) AS played_at_percent_rank_dense_rank,
        median_days_since_played_at - LAG(median_days_since_played_at) OVER (ORDER BY played_at_percent_rank DESC, id) AS median_days_since_played_at_change
    FROM outliers
    WINDOW w_all_by_played_at_percent_ran AS (ORDER BY played_at_percent_rank DESC)
)
SELECT
    id,
    listener_id,
    played_at,
    device,
    source,
    ms_played_distance,
    days_since_played_at,
    is_skipped_listener_id_max_ms_played_distance_2,
    median_days_since_played_at,
    days_since_played_at_minus_median,
    played_at_percent_rank_percent_rank,
    played_at_percent_rank_dense_rank,
    CASE WHEN device IN ('car', 'speaker') THEN 'group_a' WHEN device = 'tv' THEN 'group_b' ELSE 'other' END AS device_group,
    ROUND(played_at_percent_rank * 0.25, 2) AS played_at_percent_rank_adj
FROM metrics
WHERE days_since_played_at >= 105
ORDER BY is_skipped_listener_id_max_ms_played_distance_2 DESC, listener_id DESC, id
LIMIT 5
