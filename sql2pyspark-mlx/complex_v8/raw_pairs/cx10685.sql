WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket
    FROM plays
    WHERE device NOT IN ('tv', 'car')
),
combined_details AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device,
        s.ms_played_bucket,
        l.plan AS listener_plan,
        l.signup_date AS listener_signup_date,
        l.username AS listener_username
    FROM prepared s
    LEFT JOIN listeners l ON l.id = s.listener_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTH_VALUE(listener_signup_date, 2) OVER (PARTITION BY listener_id ORDER BY listener_signup_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_listener_signup_date,
        MAX(ms_played_bucket) OVER (PARTITION BY listener_id ORDER BY listener_signup_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_ms_played_bucket_so_far,
        LAG(ms_played_bucket, 1, 0) OVER w_all_by_played_at AS prev_ms_played_bucket,
        COUNT(*) OVER (PARTITION BY listener_id, listener_plan) AS rows_in_listener_id_listener_plan,
        LEAD(played_at) OVER (ORDER BY played_at, id) AS next_played_at,
        NTH_VALUE(listener_signup_date, 2) OVER (PARTITION BY device ORDER BY ms_played_bucket DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_listener_signup_date_2,
        MAX(ms_played_bucket) OVER (PARTITION BY listener_id ORDER BY listener_signup_date ASC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS ms_played_bucket_max_4,
        NTILE(4) OVER (ORDER BY played_at, id) AS played_at_quartile,
        RANK() OVER (ORDER BY played_at, id) AS played_at_rank,
        MAX(ms_played_bucket) OVER (PARTITION BY YEAR(listener_signup_date)) AS listener_signup_date_year_max_ms_played_bucket,
        ROW_NUMBER() OVER w_all_by_played_at AS rn
    FROM combined_details
    WINDOW w_all_by_played_at AS (ORDER BY played_at, id)
)
-- Final projection
SELECT
    id,
    listener_id,
    played_at,
    device,
    ms_played_bucket,
    listener_plan,
    listener_signup_date,
    listener_username,
    second_listener_signup_date,
    rows_in_listener_id_listener_plan,
    second_listener_signup_date_2,
    ms_played_bucket_max_4,
    played_at_rank,
    listener_signup_date_year_max_ms_played_bucket,
    rn
FROM windowed
ORDER BY listener_username, listener_plan DESC NULLS FIRST
