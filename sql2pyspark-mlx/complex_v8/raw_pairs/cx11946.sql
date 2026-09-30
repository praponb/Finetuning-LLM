WITH base AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        INITCAP(TRIM(source)) AS source_clean,
        LOWER(TRIM(device)) AS device_clean
    FROM plays
    WHERE listener_id IS NOT NULL
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY device ORDER BY played_at ASC, id), 3) AS played_at_cume_dist,
        DENSE_RANK() OVER (PARTITION BY track_id, is_skipped ORDER BY played_at) AS played_at_dense_rank,
        RANK() OVER w_device_by_played_at AS played_at_rank,
        NTILE(3) OVER w_track_id_is_skipped_by_playe_2 AS played_at_tercile,
        LAG(played_at, 2) OVER (PARTITION BY track_id, is_skipped ORDER BY played_at, id) AS prev2_played_at,
        RANK() OVER w_played_at_to_date_device_by AS played_at_rank_2,
        NTH_VALUE(played_at, 2) OVER (PARTITION BY device ORDER BY played_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at,
        ROUND(CUME_DIST() OVER (PARTITION BY track_id, is_skipped ORDER BY played_at), 3) AS played_at_cume_dist_2,
        ROUND(PERCENT_RANK() OVER (PARTITION BY device ORDER BY played_at ASC, id), 3) AS played_at_percent_rank,
        ROUND((UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(LAG(played_at) OVER w_played_at_to_date_device_by)) / 60, 1) AS minutes_since_prev_played_at,
        FIRST_VALUE(played_at) IGNORE NULLS OVER (PARTITION BY TO_DATE(played_at), device ORDER BY played_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_played_at,
        LAG(played_at) OVER w_played_at_to_date_device_by AS prev_played_at,
        LAG(played_at) OVER w_device_by_played_at AS prev_played_at_2,
        NTILE(10) OVER w_track_id_is_skipped_by_playe_2 AS played_at_decile
    FROM base
    WINDOW w_device_by_played_at AS (PARTITION BY device ORDER BY played_at ASC, id), w_track_id_is_skipped_by_playe_2 AS (PARTITION BY track_id, is_skipped ORDER BY played_at, id), w_played_at_to_date_device_by AS (PARTITION BY TO_DATE(played_at), device ORDER BY played_at ASC, id), w_track_id_is_skipped_by_playe AS (PARTITION BY track_id, is_skipped ORDER BY played_at)
),
first_rows AS (
    -- Top 3 per group
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source_clean,
        device_clean,
        played_at_cume_dist,
        played_at_rank,
        played_at_tercile,
        prev2_played_at,
        played_at_rank_2,
        second_played_at,
        played_at_cume_dist_2,
        played_at_percent_rank,
        minutes_since_prev_played_at,
        first_known_played_at,
        prev_played_at,
        prev_played_at_2,
        played_at_decile
    FROM windowed
    WHERE played_at_dense_rank <= 3
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        source_clean,
        played_at_cume_dist,
        played_at_rank,
        played_at_tercile,
        prev2_played_at,
        played_at_rank_2,
        played_at_cume_dist_2,
        played_at_percent_rank,
        minutes_since_prev_played_at,
        prev_played_at,
        prev_played_at_2,
        played_at_decile,
        ROUND(CUME_DIST() OVER (ORDER BY played_at DESC), 3) AS played_at_cume_dist_3,
        SIZE(COLLECT_SET(track_id) OVER (PARTITION BY DATE_TRUNC('MONTH', prev_played_at_2))) AS distinct_track_id_in_prev_played_at_2_month,
        LAG(played_at_rank) OVER (ORDER BY played_at DESC, id) AS prev_played_at_rank,
        MAX(played_at_cume_dist) OVER (ORDER BY prev_played_at_2 ASC NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS played_at_cume_dist_max_3
    FROM first_rows
)
-- Final projection
SELECT
    id,
    listener_id,
    device,
    source_clean,
    played_at_tercile,
    prev2_played_at,
    played_at_rank_2,
    played_at_cume_dist_2,
    played_at_percent_rank,
    minutes_since_prev_played_at,
    prev_played_at,
    prev_played_at_2,
    played_at_decile,
    played_at_cume_dist_3,
    distinct_track_id_in_prev_played_at_2_month,
    prev_played_at_rank,
    played_at_cume_dist_max_3,
    CONCAT_WS(': ', source_clean, device) AS source_clean_device_label,
    ROUND(played_at_percent_rank * 0.25, 2) AS played_at_percent_rank_adj
FROM with_trends
ORDER BY played_at_percent_rank DESC, prev_played_at_2
