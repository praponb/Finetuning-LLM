WITH recent AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        source,
        LOWER(REGEXP_REPLACE(TRIM(device), ' ', '_')) AS device_slug
    FROM plays
    WHERE source <> 'album'
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        listener_id,
        ms_played,
        device,
        device_slug,
        ROUND(AVG(ms_played) OVER (ORDER BY ms_played ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS ms_played_avg_6_prior,
        DENSE_RANK() OVER (ORDER BY ms_played ASC) AS ms_played_dense_rank,
        SIZE(COLLECT_SET(listener_id) OVER (PARTITION BY source)) AS distinct_listener_id_in_source,
        LAST_VALUE(ms_played) OVER (PARTITION BY source, listener_id ORDER BY ms_played ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_ms_played,
        LAG(ms_played) OVER (PARTITION BY track_id, device ORDER BY ms_played DESC, id) AS prev_ms_played,
        DENSE_RANK() OVER (PARTITION BY listener_id ORDER BY ms_played DESC, id) AS ms_played_dense_rank_2,
        DENSE_RANK() OVER (ORDER BY ms_played ASC, id) AS ms_played_dense_rank_3,
        MIN(ms_played) OVER (PARTITION BY source) AS source_min_ms_played
    FROM recent
),
leaders AS (
    -- Top 3 per group
    SELECT *
    FROM with_windows
    WHERE ms_played_dense_rank_2 <= 3
      AND source_min_ms_played > 1603000
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        ms_played,
        device_slug,
        ms_played_avg_6_prior,
        ms_played_dense_rank,
        last_ms_played,
        ms_played_dense_rank_2,
        ms_played_dense_rank_3,
        source_min_ms_played,
        FIRST_VALUE(ms_played) IGNORE NULLS OVER (PARTITION BY listener_id ORDER BY ms_played_dense_rank_2 DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ms_played,
        SUM(prev_ms_played) OVER (PARTITION BY device) AS device_sum_prev_ms_played,
        SIZE(COLLECT_SET(device) OVER (PARTITION BY distinct_listener_id_in_source)) AS distinct_device_in_distinct_listener_id_in_sourc,
        ROUND(ms_played_avg_6_prior - AVG(ms_played_avg_6_prior) OVER (PARTITION BY device), 2) AS ms_played_avg_6_prior_vs_device_avg,
        ROUND(CUME_DIST() OVER w_ms_played_dense_rank_2_ms_pl, 3) AS ms_played_dense_rank_2_cume_dist,
        NTH_VALUE(ms_played, 2) OVER (PARTITION BY listener_id ORDER BY ms_played_dense_rank_2 DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played,
        ROUND(PERCENT_RANK() OVER (PARTITION BY ms_played_dense_rank_2, ms_played_dense_rank_3 ORDER BY ms_played_dense_rank_2 ASC), 3) AS ms_played_dense_rank_2_percent_rank,
        SUM(ms_played_dense_rank_3) OVER (PARTITION BY distinct_listener_id_in_source) AS distinct_listener_id_in_source_sum_ms_played_den
    FROM leaders
    WINDOW w_ms_played_dense_rank_2_ms_pl AS (PARTITION BY ms_played_dense_rank_2, ms_played_dense_rank_3 ORDER BY ms_played_dense_rank_2 ASC)
)
-- Final projection
SELECT
    id,
    ms_played,
    device_slug,
    ms_played_avg_6_prior,
    last_ms_played,
    ms_played_dense_rank_2,
    ms_played_dense_rank_3,
    source_min_ms_played,
    first_known_ms_played,
    second_ms_played,
    distinct_listener_id_in_source_sum_ms_played_den
FROM metrics
