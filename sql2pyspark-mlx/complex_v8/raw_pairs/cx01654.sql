WITH recent_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        CASE WHEN ms_played >= 239955 THEN 'large' WHEN ms_played >= 71476 THEN 'medium' ELSE 'small' END AS ms_played_tier,
        ABS(ms_played - 192595) AS ms_played_distance
    FROM plays
    WHERE is_skipped = TRUE
      AND device = 'tv'
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MIN(ms_played) OVER w_listener_id_track_id AS listener_id_track_id_min_ms_played,
        SUM(ms_played_distance) OVER (PARTITION BY source ORDER BY played_at ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS ms_played_distance_sum_1_prior,
        SIZE(COLLECT_SET(device) OVER (PARTITION BY listener_id, track_id)) AS distinct_device_in_listener_id_track_id,
        NTILE(10) OVER (ORDER BY ms_played_distance ASC NULLS LAST, id) AS ms_played_distance_decile,
        NTILE(10) OVER (PARTITION BY source ORDER BY ms_played_distance, id) AS ms_played_distance_decile_2,
        NTILE(10) OVER w_is_skipped_by_ms_played AS ms_played_decile,
        LEAD(ms_played_distance) OVER (PARTITION BY source ORDER BY ms_played_distance, id) AS next_ms_played_distance,
        RANK() OVER w_is_skipped_by_ms_played AS ms_played_rank,
        RANK() OVER (PARTITION BY source ORDER BY played_at ASC) AS played_at_rank,
        ROUND(AVG(ms_played) OVER (PARTITION BY source ORDER BY played_at ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS ms_played_avg_6_prior,
        LAST_VALUE(played_at) IGNORE NULLS OVER (ORDER BY ms_played_distance ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_played_at,
        FIRST_VALUE(ms_played) IGNORE NULLS OVER (PARTITION BY source ORDER BY ms_played_distance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ms_played,
        ROUND(AVG(ms_played_distance) OVER (PARTITION BY source ORDER BY played_at ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS ms_played_distance_avg_2
    FROM recent_plays
    WINDOW w_listener_id_track_id AS (PARTITION BY listener_id, track_id), w_is_skipped_by_ms_played AS (PARTITION BY is_skipped ORDER BY ms_played DESC, id)
),
grouped AS (
    -- Group-level totals
    SELECT
        device,
        ms_played_distance_decile,
        listener_id,
        MAX(last_known_played_at) AS last_last_known_played_at,
        COUNT(*) AS cnt,
        MIN(last_known_played_at) AS first_last_known_played_at,
        COUNT(DISTINCT id) AS distinct_ids,
        SUM(CASE WHEN device IS NOT NULL THEN 1 ELSE 0 END) AS n_device_is_not_null
    FROM windowed
    WHERE MONTH(last_known_played_at) IN (1, 2)
    GROUP BY device, ms_played_distance_decile, listener_id
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTH_VALUE(n_device_is_not_null, 2) OVER (ORDER BY first_last_known_played_at DESC NULLS FIRST, device, ms_played_distance_decile, listener_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_n_device_is_not_null,
        LAST_VALUE(last_last_known_played_at) OVER (ORDER BY first_last_known_played_at DESC NULLS FIRST, device, ms_played_distance_decile, listener_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_last_last_known_played_at
    FROM grouped
)
SELECT
    device,
    ms_played_distance_decile,
    last_last_known_played_at,
    first_last_known_played_at,
    n_device_is_not_null,
    second_n_device_is_not_null,
    last_last_last_known_played_at
FROM with_windows
ORDER BY last_last_last_known_played_at NULLS LAST
