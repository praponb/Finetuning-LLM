WITH recent_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source,
        CASE WHEN ms_played >= 299017 THEN 'large' WHEN ms_played >= 160264 THEN 'medium' ELSE 'small' END AS ms_played_tier,
        DATE_FORMAT(played_at, 'MMM yyyy') AS played_at_label
    FROM plays
    WHERE (device <> 'tv' OR listener_id <= 19)
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY device, source ORDER BY played_at, id) AS played_at_seq,
        ROUND(AVG(ms_played) OVER (PARTITION BY ms_played_tier), 2) AS ms_played_tier_avg_ms_played,
        ROUND(AVG(ms_played) OVER (ORDER BY played_at ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS ms_played_avg_2,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER (PARTITION BY ms_played_tier), 0), 4) AS ms_played_share_of_ms_played_tier,
        LEAD(played_at) OVER (ORDER BY played_at ASC, id) AS next_played_at,
        NTH_VALUE(played_at, 2) OVER (ORDER BY played_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at,
        COUNT(*) OVER (PARTITION BY track_id) AS rows_in_track_id,
        LAST_VALUE(ms_played) OVER (ORDER BY played_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_ms_played,
        SUM(ms_played) OVER (PARTITION BY device ORDER BY played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ms_played,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY ms_played_tier), 2) AS ms_played_vs_ms_played_tier_avg
    FROM recent_plays
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        ms_played_tier,
        played_at_seq,
        ms_played_tier_avg_ms_played,
        ms_played_avg_2,
        ms_played_share_of_ms_played_tier,
        next_played_at,
        rows_in_track_id,
        last_ms_played,
        running_ms_played,
        ROUND(PERCENT_RANK() OVER (ORDER BY ms_played_vs_ms_played_tier_avg DESC), 3) AS ms_played_vs_ms_played_tier_avg_percent_rank,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER (PARTITION BY played_at_seq), 0), 4) AS ms_played_share_of_played_at_seq,
        LAG(rows_in_track_id, 1, 0) OVER w_played_at_month_device_by_la AS prev_rows_in_track_id,
        ROUND(AVG(rows_in_track_id) OVER w_played_at_seq, 2) AS played_at_seq_avg_rows_in_track_id,
        ROUND(CUME_DIST() OVER (PARTITION BY MONTH(played_at), device ORDER BY last_ms_played DESC), 3) AS last_ms_played_cume_dist,
        ROW_NUMBER() OVER (ORDER BY ms_played_vs_ms_played_tier_avg DESC, id) AS ms_played_vs_ms_played_tier_avg_seq
    FROM sequenced
    WINDOW w_played_at_month_device_by_la AS (PARTITION BY MONTH(played_at), device ORDER BY last_ms_played DESC, id), w_played_at_seq AS (PARTITION BY played_at_seq)
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        ms_played_tier,
        ROUND(SUM(last_ms_played_cume_dist), 2) AS total_last_ms_played_cume_dist,
        ROUND(STDDEV(last_ms_played_cume_dist), 2) AS stddev_last_ms_played_cume_dist
    FROM metrics
    WHERE prev_rows_in_track_id > 9
    GROUP BY 1
    HAVING MAX(ms_played_avg_2) > 143250.0
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY ms_played_tier ORDER BY total_last_ms_played_cume_dist), 3) AS total_last_ms_played_cume_dist_percent_rank
    FROM summary
)
-- Final projection
SELECT
    ms_played_tier,
    total_last_ms_played_cume_dist,
    stddev_last_ms_played_cume_dist,
    total_last_ms_played_cume_dist_percent_rank,
    LENGTH(TRIM(ms_played_tier)) AS ms_played_tier_len
FROM windowed
WHERE total_last_ms_played_cume_dist_percent_rank > 0.2
ORDER BY ms_played_tier_len DESC, stddev_last_ms_played_cume_dist NULLS LAST, ms_played_tier
LIMIT 20
