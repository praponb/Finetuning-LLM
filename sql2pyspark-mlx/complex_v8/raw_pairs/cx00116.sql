WITH base AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        COALESCE(ms_played, 0) AS ms_played_filled,
        CASE WHEN ms_played >= 299017 THEN 'A' WHEN ms_played >= 73335 THEN 'B' ELSE 'C' END AS ms_played_tier
    FROM plays
    WHERE listener_id IS NOT NULL
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        ms_played_filled,
        ms_played_tier,
        SUM(ms_played_filled) OVER (PARTITION BY device, listener_id ORDER BY ms_played DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ms_played_filled_sum_7,
        MAX(ms_played) OVER (PARTITION BY source, DATE_TRUNC('MONTH', played_at) ORDER BY ms_played ASC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS ms_played_max_3_prior,
        NTH_VALUE(played_at, 2) OVER (PARTITION BY source, DATE_TRUNC('MONTH', played_at) ORDER BY ms_played ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at,
        ms_played_filled - LAG(ms_played_filled) OVER (PARTITION BY source, DATE_TRUNC('MONTH', played_at) ORDER BY ms_played ASC, id) AS ms_played_filled_change,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY ms_played_tier, track_id), 2) AS ms_played_vs_ms_played_tier_track_id_avg,
        SUM(ms_played_filled) OVER (PARTITION BY ms_played_tier, track_id) AS ms_played_tier_track_id_sum_ms_played_filled,
        ROUND(AVG(ms_played_filled) OVER (PARTITION BY ms_played_tier, track_id), 2) AS ms_played_tier_track_id_avg_ms_played_filled,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER (PARTITION BY ms_played_tier, track_id), 0), 4) AS ms_played_share_of_ms_played_tier_track_id,
        SIZE(COLLECT_SET(device) OVER w_ms_played_tier_track_id) AS distinct_device_in_ms_played_tier_track_id,
        ROUND(PERCENT_RANK() OVER (PARTITION BY device, listener_id ORDER BY ms_played DESC), 3) AS ms_played_percent_rank,
        ROUND(ms_played_filled - AVG(ms_played_filled) OVER w_ms_played_tier_track_id, 2) AS ms_played_filled_vs_ms_played_tier_track_id_avg,
        ROUND(ms_played_filled / NULLIF(SUM(ms_played_filled) OVER (PARTITION BY ms_played_tier, track_id), 0), 4) AS ms_played_filled_share_of_ms_played_tier_track_i
    FROM base
    WINDOW w_ms_played_tier_track_id AS (PARTITION BY ms_played_tier, track_id)
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        is_skipped,
        source,
        ms_played_filled,
        ms_played_filled_sum_7,
        ms_played_max_3_prior,
        second_played_at,
        ms_played_filled_change,
        ms_played_vs_ms_played_tier_track_id_avg,
        ms_played_tier_track_id_sum_ms_played_filled,
        ms_played_tier_track_id_avg_ms_played_filled,
        distinct_device_in_ms_played_tier_track_id,
        ms_played_percent_rank,
        ms_played_filled_vs_ms_played_tier_track_id_avg,
        ms_played_filled_share_of_ms_played_tier_track_i,
        DENSE_RANK() OVER (ORDER BY ms_played_tier_track_id_avg_ms_played_filled DESC, id) AS ms_played_tier_track_id_avg_ms_played_filled_den,
        ROUND(ms_played_filled - AVG(ms_played_filled) OVER (PARTITION BY track_id), 2) AS ms_played_filled_vs_track_id_avg,
        FIRST_VALUE(ms_played) OVER (ORDER BY ms_played_vs_ms_played_tier_track_id_avg DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_ms_played,
        ROUND(AVG(ms_played_filled) OVER (PARTITION BY track_id), 2) AS track_id_avg_ms_played_filled,
        LAG(ms_played_percent_rank, 2) OVER (ORDER BY ms_played_tier_track_id_avg_ms_played_filled DESC, id) AS prev2_ms_played_percent_rank,
        SUM(ms_played_filled_change) OVER (PARTITION BY track_id ORDER BY second_played_at, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS ms_played_filled_change_sum_2
    FROM windowed
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    ms_played,
    source,
    ms_played_max_3_prior,
    ms_played_vs_ms_played_tier_track_id_avg,
    ms_played_tier_track_id_sum_ms_played_filled,
    ms_played_tier_track_id_avg_ms_played_filled,
    distinct_device_in_ms_played_tier_track_id,
    ms_played_percent_rank,
    ms_played_filled_vs_ms_played_tier_track_id_avg,
    ms_played_filled_share_of_ms_played_tier_track_i,
    ms_played_filled_vs_track_id_avg,
    first_ms_played,
    track_id_avg_ms_played_filled,
    prev2_ms_played_percent_rank,
    DATE_TRUNC('YEAR', second_played_at) AS second_played_at_year,
    ROUND(ms_played_percent_rank * 0.25, 2) AS ms_played_percent_rank_adj
FROM sequenced
ORDER BY ms_played_filled_vs_ms_played_tier_track_id_avg DESC NULLS FIRST, second_played_at_year DESC
