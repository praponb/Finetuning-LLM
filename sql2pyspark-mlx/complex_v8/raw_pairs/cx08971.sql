WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        source,
        CASE WHEN device IN ('speaker', 'car') THEN 'group_a' WHEN device = 'tv' THEN 'group_b' ELSE 'other' END AS device_group,
        CASE WHEN ms_played >= 220498 THEN 'HIGH' WHEN ms_played >= 157900 THEN 'MEDIUM' ELSE 'LOW' END AS ms_played_tier,
        HOUR(played_at) AS played_at_hour
    FROM plays
    WHERE device = 'car'
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY listener_id ORDER BY played_at_hour) AS played_at_hour_dense_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY device_group ORDER BY played_at_hour), 3) AS played_at_hour_percent_rank,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY played_at_hour, source), 2) AS ms_played_vs_played_at_hour_source_avg,
        ROW_NUMBER() OVER w_listener_id_by_played_at_hou_2 AS rn,
        LAST_VALUE(played_at_hour) OVER (ORDER BY played_at_hour ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_played_at_hour,
        FIRST_VALUE(played_at_hour) OVER (ORDER BY played_at_hour ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_played_at_hour
    FROM base
    WINDOW w_listener_id_by_played_at_hou_2 AS (PARTITION BY listener_id ORDER BY played_at_hour, id)
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAST_VALUE(rn) OVER (ORDER BY played_at_hour_dense_rank DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_rn,
        ROUND(AVG(played_at_hour_dense_rank) OVER (PARTITION BY listener_id), 2) AS listener_id_avg_played_at_hour_dense_rank,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY listener_id), 2) AS ms_played_vs_listener_id_avg,
        NTILE(5) OVER (ORDER BY rn, id) AS rn_quintile
    FROM with_windows
)
SELECT
    id,
    listener_id,
    track_id,
    ms_played,
    device,
    source,
    device_group,
    ms_played_tier,
    played_at_hour,
    played_at_hour_dense_rank,
    played_at_hour_percent_rank,
    ms_played_vs_played_at_hour_source_avg,
    rn,
    last_played_at_hour,
    first_played_at_hour,
    last_rn,
    listener_id_avg_played_at_hour_dense_rank,
    ms_played_vs_listener_id_avg,
    CASE WHEN first_played_at_hour >= 13 THEN 'large' WHEN first_played_at_hour >= 9 THEN 'medium' ELSE 'small' END AS first_played_at_hour_tier,
    ROUND(played_at_hour / NULLIF(first_played_at_hour, 0), 3) AS played_at_hour_per_first_played_at_hour
FROM metrics
ORDER BY last_rn, listener_id_avg_played_at_hour_dense_rank DESC
