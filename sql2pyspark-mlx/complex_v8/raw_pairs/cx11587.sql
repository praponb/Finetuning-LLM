WITH prepared AS (
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
        CASE WHEN device IN ('speaker', 'tv') THEN 'group_a' WHEN device = 'car' THEN 'group_b' ELSE 'other' END AS device_group,
        DATE_TRUNC('DAY', played_at) AS played_at_day
    FROM plays
    WHERE source NOT IN ('playlist', 'radio')
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        LEAD(played_at) OVER (ORDER BY ms_played, id) AS next_played_at,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY listener_id), 2) AS ms_played_vs_listener_id_avg,
        LAG(played_at_day) OVER (ORDER BY played_at DESC, id) AS prev_played_at_day,
        ROUND(CUME_DIST() OVER w_listener_id_device_by_ms_pla, 3) AS ms_played_cume_dist,
        LAST_VALUE(played_at) OVER (PARTITION BY is_skipped, track_id ORDER BY ms_played DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_played_at,
        ROUND(CUME_DIST() OVER (PARTITION BY is_skipped ORDER BY played_at, id), 3) AS played_at_cume_dist,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER (PARTITION BY listener_id, device_group), 0), 4) AS ms_played_share_of_listener_id_device_group,
        LAG(played_at) OVER (PARTITION BY is_skipped ORDER BY played_at, id) AS prev_played_at,
        ROUND(PERCENT_RANK() OVER w_listener_id_device_by_ms_pla, 3) AS ms_played_percent_rank,
        MAX(ms_played) OVER (PARTITION BY listener_id, device_group) AS listener_id_device_group_max_ms_played,
        FIRST_VALUE(played_at) OVER (PARTITION BY listener_id, YEAR(played_at) ORDER BY played_at_day, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_played_at,
        NTILE(10) OVER (PARTITION BY is_skipped, track_id ORDER BY ms_played DESC, id) AS ms_played_decile
    FROM prepared
    WINDOW w_listener_id_device_by_ms_pla AS (PARTITION BY listener_id, device ORDER BY ms_played ASC, id)
)
SELECT DISTINCT first_played_at, next_played_at, source
FROM windowed
ORDER BY source DESC
