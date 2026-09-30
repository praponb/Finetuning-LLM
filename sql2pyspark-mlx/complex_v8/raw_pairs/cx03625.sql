WITH cleaned_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM plays
),
joined AS (
    -- Attach track attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device,
        s.source,
        t.is_explicit AS track_is_explicit,
        t.title AS track_title,
        t.duration_sec AS track_duration_sec
    FROM cleaned_plays s
    LEFT JOIN tracks t ON t.id = s.track_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        played_at,
        device,
        source,
        track_is_explicit,
        track_duration_sec,
        MIN(track_duration_sec) OVER (PARTITION BY device) AS device_min_track_duration_sec,
        DENSE_RANK() OVER (PARTITION BY YEAR(played_at) ORDER BY played_at NULLS LAST, id) AS played_at_dense_rank,
        SIZE(COLLECT_SET(listener_id) OVER (PARTITION BY device)) AS distinct_listener_id_in_device,
        COUNT(*) OVER w_device AS rows_in_device,
        LAG(track_duration_sec, 2, 0) OVER (PARTITION BY track_is_explicit ORDER BY played_at, id) AS prev2_track_duration_sec,
        ROUND(PERCENT_RANK() OVER (ORDER BY track_duration_sec), 3) AS track_duration_sec_percent_rank,
        LAG(played_at) OVER w_played_at_year_by_played_at AS prev_played_at,
        LAST_VALUE(played_at) OVER (ORDER BY track_duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_played_at,
        ROUND((UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(LAG(played_at) OVER (PARTITION BY YEAR(played_at) ORDER BY played_at NULLS LAST, id))) / 60, 1) AS minutes_since_prev_played_at,
        SUM(track_duration_sec) OVER (PARTITION BY track_is_explicit ORDER BY played_at, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS track_duration_sec_sum_3,
        NTH_VALUE(track_duration_sec, 2) OVER (ORDER BY track_duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_track_duration_sec,
        LAG(track_duration_sec, 3) OVER (PARTITION BY YEAR(played_at) ORDER BY played_at NULLS LAST, id) AS prev3_track_duration_sec,
        LEAD(track_duration_sec, 1, 0) OVER (PARTITION BY YEAR(played_at) ORDER BY played_at NULLS LAST, id) AS next_track_duration_sec
    FROM joined
    WINDOW w_device AS (PARTITION BY device), w_played_at_year_by_played_at AS (PARTITION BY YEAR(played_at) ORDER BY played_at NULLS LAST, id)
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(prev_played_at) - UNIX_TIMESTAMP(LAG(prev_played_at) OVER (PARTITION BY track_is_explicit ORDER BY last_played_at, id))) / 60, 1) AS minutes_since_prev_prev_played_at,
        NTILE(5) OVER (ORDER BY played_at_dense_rank DESC, id) AS played_at_dense_rank_quintile,
        ROUND(AVG(minutes_since_prev_played_at) OVER (ORDER BY played_at_dense_rank DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS minutes_since_prev_played_at_avg_4,
        ROUND(next_track_duration_sec - AVG(next_track_duration_sec) OVER (PARTITION BY device, source), 2) AS next_track_duration_sec_vs_device_source_avg
    FROM with_trends
)
-- Final projection
SELECT
    id,
    source,
    track_is_explicit,
    track_duration_sec,
    played_at_dense_rank,
    distinct_listener_id_in_device,
    prev2_track_duration_sec,
    prev_played_at,
    last_played_at,
    minutes_since_prev_played_at,
    track_duration_sec_sum_3,
    second_track_duration_sec,
    next_track_duration_sec,
    minutes_since_prev_prev_played_at,
    played_at_dense_rank_quintile,
    next_track_duration_sec_vs_device_source_avg,
    COALESCE(device, 'UNKNOWN') AS device_or_unknown,
    ABS(played_at_dense_rank - 4) AS played_at_dense_rank_distance
FROM with_windows
ORDER BY minutes_since_prev_prev_played_at DESC, played_at_dense_rank_quintile
