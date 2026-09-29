WITH base_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        LENGTH(TRIM(device)) AS device_len
    FROM plays
    WHERE device = 'phone'
),
joined_listener AS (
    -- Attach listener attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.device,
        l.plan AS listener_plan,
        l.birth_year AS listener_birth_year,
        l.username AS listener_username
    FROM base_plays s
    JOIN listeners l ON l.id = s.listener_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        ms_played,
        device,
        listener_plan,
        listener_username,
        MIN(ms_played) OVER (PARTITION BY device) AS device_min_ms_played,
        ROUND(PERCENT_RANK() OVER (ORDER BY listener_birth_year, id), 3) AS listener_birth_year_percent_rank,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER w_listener_plan_listener_id, 0), 4) AS ms_played_share_of_listener_plan_listener_id,
        FIRST_VALUE(listener_birth_year) OVER (PARTITION BY track_id ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_listener_birth_year,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY listener_plan, listener_id), 2) AS ms_played_vs_listener_plan_listener_id_avg,
        FIRST_VALUE(ms_played) IGNORE NULLS OVER (PARTITION BY track_id ORDER BY listener_birth_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ms_played,
        LAST_VALUE(ms_played) IGNORE NULLS OVER (PARTITION BY listener_plan, track_id ORDER BY ms_played DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_ms_played,
        SUM(listener_birth_year) OVER (PARTITION BY listener_plan, track_id ORDER BY ms_played DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_listener_birth_year,
        ROUND(PERCENT_RANK() OVER (PARTITION BY track_id ORDER BY listener_birth_year DESC), 3) AS listener_birth_year_percent_rank_2
    FROM joined_listener
    WINDOW w_listener_plan_listener_id AS (PARTITION BY listener_plan, listener_id)
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        listener_id,
        ms_played,
        device,
        listener_plan,
        listener_username,
        device_min_ms_played,
        listener_birth_year_percent_rank,
        first_listener_birth_year,
        ms_played_vs_listener_plan_listener_id_avg,
        first_known_ms_played,
        running_listener_birth_year,
        listener_birth_year_percent_rank_2,
        FIRST_VALUE(device_min_ms_played) IGNORE NULLS OVER (PARTITION BY listener_plan ORDER BY device_min_ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_device_min_ms_played,
        SUM(last_known_ms_played) OVER (PARTITION BY listener_plan) AS listener_plan_sum_last_known_ms_played,
        MAX(running_listener_birth_year) OVER (PARTITION BY listener_plan ORDER BY device_min_ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_running_listener_birth_year_so_far,
        SIZE(COLLECT_SET(listener_id) OVER (PARTITION BY listener_plan)) AS distinct_listener_id_in_listener_plan,
        MAX(first_known_ms_played) OVER (PARTITION BY listener_plan) AS listener_plan_max_first_known_ms_played
    FROM windowed
)
-- Final projection
SELECT
    id,
    listener_id,
    ms_played,
    device,
    listener_plan,
    device_min_ms_played,
    listener_birth_year_percent_rank,
    first_listener_birth_year,
    ms_played_vs_listener_plan_listener_id_avg,
    first_known_ms_played,
    running_listener_birth_year,
    listener_birth_year_percent_rank_2,
    first_known_device_min_ms_played,
    max_running_listener_birth_year_so_far,
    listener_plan_max_first_known_ms_played,
    SUBSTRING(TRIM(listener_plan), 1, 2) AS listener_plan_prefix,
    LENGTH(TRIM(listener_username)) AS listener_username_len
FROM with_windows
ORDER BY device_min_ms_played NULLS LAST, first_listener_birth_year DESC
