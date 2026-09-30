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
        COALESCE(ms_played, 0) AS ms_played_filled,
        CASE WHEN is_skipped THEN 1 ELSE 0 END AS is_skipped_flag,
        ABS(ms_played - 157900) AS ms_played_distance
    FROM plays
    WHERE source = 'album'
),
exploded_detail AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.source,
        s.ms_played_filled,
        p.id AS play_id,
        p.played_at AS play_played_at,
        p.device AS play_device
    FROM prepared s
    JOIN plays p ON p.listener_id = s.listener_id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        listener_id,
        ms_played,
        source,
        ms_played_filled,
        play_id,
        play_played_at,
        play_device,
        LAST_VALUE(ms_played_filled) OVER (PARTITION BY MONTH(play_played_at), source ORDER BY ms_played, id, play_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_ms_played_filled,
        RANK() OVER w_all_by_play_played_at_ties AS play_played_at_rank,
        MAX(ms_played_filled) OVER (ORDER BY play_played_at, id, play_id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS ms_played_filled_max_3
    FROM exploded_detail
    WINDOW w_all_by_play_played_at_ties AS (ORDER BY play_played_at)
),
joined_listener AS (
    -- Attach listener attributes
    SELECT
        s.*,
        l.signup_date AS listener_signup_date,
        l.plan AS listener_plan,
        l.country AS listener_country,
        p.ms_played AS play_ms_played
    FROM with_windows s
    JOIN listeners l ON l.id = s.listener_id
    JOIN plays p ON p.id = s.play_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT play_device, source, listener_country, ms_played_filled_max_3
    FROM joined_listener
) src
PIVOT (
    MIN(ms_played_filled_max_3) AS min_v, SUM(ms_played_filled_max_3) AS sum_v
    FOR listener_country IN ('TH', 'BR')
)
ORDER BY play_device, source
