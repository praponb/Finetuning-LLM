WITH base_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM plays
    WHERE played_at < TIMESTAMP '2025-02-18 10:00:00'
      AND track_id IS NOT NULL
),
joined AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device,
        s.is_skipped,
        s.source,
        l.signup_date AS listener_signup_date,
        l.country AS listener_country,
        t.is_explicit AS track_is_explicit
    FROM base_plays s
    LEFT JOIN listeners l ON l.id = s.listener_id
    LEFT JOIN tracks t ON t.id = s.track_id
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(played_at, 2) OVER (PARTITION BY listener_country, source ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at,
        NTH_VALUE(listener_signup_date, 2) OVER (ORDER BY listener_signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_listener_signup_date,
        LEAD(played_at, 3) OVER w_all_by_listener_signup_date_2 AS next3_played_at,
        LAST_VALUE(played_at) OVER (PARTITION BY listener_country, source ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_played_at,
        ROUND(CUME_DIST() OVER w_all_by_listener_signup_date_3, 3) AS listener_signup_date_cume_dist
    FROM joined
    WINDOW w_all_by_listener_signup_date_2 AS (ORDER BY listener_signup_date, id), w_all_by_listener_signup_date_3 AS (ORDER BY listener_signup_date)
),
device_stats AS (
    -- Avg listener_signup_date_cume_dist per device
    SELECT device, AVG(listener_signup_date_cume_dist) AS avg_listener_signup_date_cume_dist
    FROM with_windows
    GROUP BY device
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_listener_signup_date_cume_dist,
        ROUND(s.listener_signup_date_cume_dist - g.avg_listener_signup_date_cume_dist, 2) AS listener_signup_date_cume_dist_minus_avg
    FROM with_windows s
    JOIN device_stats g ON g.device = s.device
    WHERE s.listener_signup_date_cume_dist > g.avg_listener_signup_date_cume_dist
),
joined_listener AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        l.country AS listener_country_2,
        l.interests AS listener_interests,
        t.bpm AS track_bpm,
        t.duration_sec AS track_duration_sec
    FROM outliers s
    LEFT JOIN listeners l ON l.id = s.listener_id
    JOIN tracks t ON t.id = s.track_id
),
with_parent_listener AS (
    -- Attach listener attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device,
        s.is_skipped,
        s.source,
        s.listener_signup_date,
        s.listener_country,
        s.track_is_explicit,
        s.second_listener_signup_date,
        s.listener_signup_date_cume_dist,
        s.avg_listener_signup_date_cume_dist,
        s.listener_signup_date_cume_dist_minus_avg,
        s.listener_country_2,
        s.listener_interests,
        s.track_bpm,
        s.track_duration_sec,
        l.signup_date AS listener_signup_date_2
    FROM joined_listener s
    LEFT JOIN listeners l ON l.id = s.listener_id AND l.birth_year < 1990
)
SELECT
    id,
    played_at,
    is_skipped,
    source,
    listener_signup_date,
    track_is_explicit,
    second_listener_signup_date,
    avg_listener_signup_date_cume_dist,
    listener_interests,
    listener_signup_date_2,
    track_duration_sec - track_bpm AS track_duration_sec_minus_track_bpm,
    ABS(track_duration_sec - 209) AS track_duration_sec_distance
FROM with_parent_listener
WHERE is_skipped = TRUE
