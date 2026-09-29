WITH valid AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        source
    FROM plays
    WHERE device NOT IN ('car', 'phone')
),
with_details_listener AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.source,
        l.country AS listener_country,
        l.signup_date AS listener_signup_date,
        t.title AS track_title,
        t.duration_sec AS track_duration_sec
    FROM valid s
    LEFT JOIN listeners l ON l.id = s.listener_id
    JOIN tracks t ON t.id = s.track_id
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(track_duration_sec) OVER (PARTITION BY track_id, source ORDER BY track_duration_sec NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS track_duration_sec_max_2,
        ROUND(track_duration_sec / NULLIF(SUM(track_duration_sec) OVER (PARTITION BY listener_country), 0), 4) AS track_duration_sec_share_of_listener_country,
        COUNT(*) OVER w_listener_country AS rows_in_listener_country,
        ROUND(PERCENT_RANK() OVER (PARTITION BY track_id, source ORDER BY track_duration_sec NULLS LAST, id), 3) AS track_duration_sec_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY track_id, source ORDER BY track_duration_sec NULLS LAST, id) AS track_duration_sec_seq,
        NTILE(3) OVER (PARTITION BY listener_country ORDER BY track_duration_sec DESC, id) AS track_duration_sec_tercile,
        SUM(track_duration_sec) OVER (PARTITION BY track_id, source ORDER BY track_duration_sec NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS track_duration_sec_sum_3_prior,
        NTH_VALUE(track_duration_sec, 2) OVER (PARTITION BY track_id ORDER BY track_duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_track_duration_sec,
        ROUND(CUME_DIST() OVER (PARTITION BY listener_country ORDER BY track_duration_sec DESC, id), 3) AS track_duration_sec_cume_dist,
        ROUND(AVG(track_duration_sec) OVER (ORDER BY listener_signup_date, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS track_duration_sec_avg_1_prior
    FROM with_details_listener
    WINDOW w_listener_country AS (PARTITION BY listener_country)
)
SELECT
    id,
    listener_id,
    track_id,
    source,
    listener_country,
    listener_signup_date,
    track_title,
    track_duration_sec,
    track_duration_sec_share_of_listener_country,
    rows_in_listener_country,
    track_duration_sec_seq,
    track_duration_sec_tercile,
    track_duration_sec_sum_3_prior,
    second_track_duration_sec,
    track_duration_sec_cume_dist,
    track_duration_sec_avg_1_prior,
    CASE WHEN track_duration_sec_max_2 >= 1026 THEN 'large' WHEN track_duration_sec_max_2 >= 441 THEN 'medium' ELSE 'small' END AS track_duration_sec_max_2_tier
FROM ranked
ORDER BY id DESC
