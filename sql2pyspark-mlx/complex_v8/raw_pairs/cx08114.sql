WITH prepared AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        SUBSTRING(TRIM(device), 1, 2) AS device_prefix,
        SUBSTRING(TRIM(source), 1, 2) AS source_prefix
    FROM plays
    WHERE source NOT IN ('playlist', 'search')
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        NTH_VALUE(ms_played, 2) OVER (PARTITION BY track_id, listener_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played,
        ROUND(CUME_DIST() OVER (PARTITION BY track_id, listener_id ORDER BY played_at), 3) AS played_at_cume_dist,
        ms_played - LAG(ms_played) OVER w_device_listener_id_by_played AS ms_played_change,
        RANK() OVER (PARTITION BY device, listener_id ORDER BY played_at DESC) AS played_at_rank,
        SUM(ms_played) OVER (PARTITION BY listener_id, device) AS listener_id_device_sum_ms_played,
        LEAD(ms_played, 2) OVER w_all_by_played_at AS next2_ms_played,
        ROUND(AVG(ms_played) OVER (PARTITION BY listener_id, device), 2) AS listener_id_device_avg_ms_played,
        COUNT(*) OVER (PARTITION BY YEAR(played_at) ORDER BY played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM prepared
    WINDOW w_device_listener_id_by_played AS (PARTITION BY device, listener_id ORDER BY played_at DESC, id), w_all_by_played_at AS (ORDER BY played_at ASC, id)
),
leaders AS (
    -- Top 3 per group
    SELECT *
    FROM windowed
    WHERE played_at_rank <= 3
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        played_at,
        ms_played,
        device,
        device_prefix,
        source_prefix,
        played_at_cume_dist,
        ms_played_change,
        listener_id_device_sum_ms_played,
        listener_id_device_avg_ms_played,
        running_count,
        ROUND(PERCENT_RANK() OVER (ORDER BY running_count DESC NULLS FIRST, id), 3) AS running_count_percent_rank,
        SIZE(COLLECT_SET(device) OVER w_track_id_listener_id) AS distinct_device_in_track_id_listener_id,
        DENSE_RANK() OVER (PARTITION BY played_at_rank ORDER BY running_count DESC) AS running_count_dense_rank,
        RANK() OVER (PARTITION BY device ORDER BY played_at_rank DESC, id) AS played_at_rank_rank,
        SUM(ms_played_change) OVER (PARTITION BY track_id, listener_id) AS track_id_listener_id_sum_ms_played_change,
        SUM(played_at_rank) OVER w_track_id_listener_id AS track_id_listener_id_sum_played_at_rank
    FROM leaders
    WINDOW w_track_id_listener_id AS (PARTITION BY track_id, listener_id)
)
SELECT DISTINCT running_count_dense_rank
FROM with_trends
WHERE listener_id_device_avg_ms_played <= 242000.0
ORDER BY running_count_dense_rank
