WITH valid AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        device,
        is_skipped,
        source,
        DATE_TRUNC('WEEK', played_at) AS played_at_week,
        ABS(ms_played - 166517) AS ms_played_distance
    FROM plays
    WHERE listener_id <= 5
      AND device IN ('car', 'tv')
),
joined_listener AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.source,
        l.country AS listener_country,
        l.birth_year AS listener_birth_year,
        l.signup_date AS listener_signup_date,
        t.duration_sec AS track_duration_sec
    FROM valid s
    LEFT JOIN listeners l ON l.id = s.listener_id AND l.birth_year <= 1980
    LEFT JOIN tracks t ON t.id = s.track_id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(track_duration_sec) OVER w_device_by_listener_signup_da AS prev_track_duration_sec,
        LAST_VALUE(track_duration_sec) OVER (PARTITION BY listener_country, source ORDER BY track_duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_track_duration_sec,
        COUNT(*) OVER w_track_id_listener_country AS rows_in_track_id_listener_country,
        MAX(track_duration_sec) OVER (PARTITION BY listener_id, listener_country ORDER BY track_duration_sec, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS track_duration_sec_max_3,
        ROUND(track_duration_sec / NULLIF(SUM(track_duration_sec) OVER (PARTITION BY track_id, listener_country), 0), 4) AS track_duration_sec_share_of_track_id_listener_co,
        ROUND(PERCENT_RANK() OVER (PARTITION BY listener_id, listener_country ORDER BY track_duration_sec, id), 3) AS track_duration_sec_percent_rank,
        DENSE_RANK() OVER (PARTITION BY listener_country, source ORDER BY track_duration_sec DESC, id) AS track_duration_sec_dense_rank,
        MAX(listener_birth_year) OVER (PARTITION BY source) AS source_max_listener_birth_year,
        LEAD(listener_birth_year) OVER (PARTITION BY device ORDER BY listener_signup_date DESC, id) AS next_listener_birth_year,
        RANK() OVER w_device_by_listener_signup_da_2 AS listener_signup_date_rank,
        ROUND(listener_birth_year - AVG(listener_birth_year) OVER w_source, 2) AS listener_birth_year_vs_source_avg,
        COUNT(*) OVER (PARTITION BY track_id, source) AS rows_in_track_id_source,
        SUM(listener_birth_year) OVER (PARTITION BY DATE_TRUNC('MONTH', listener_signup_date) ORDER BY listener_signup_date, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS listener_birth_year_sum_2_prior
    FROM joined_listener
    WINDOW w_device_by_listener_signup_da AS (PARTITION BY device ORDER BY listener_signup_date DESC, id), w_track_id_listener_country AS (PARTITION BY track_id, listener_country), w_device_by_listener_signup_da_2 AS (PARTITION BY device ORDER BY listener_signup_date DESC), w_source AS (PARTITION BY source)
)
-- Final projection
SELECT
    id,
    last_track_duration_sec,
    rows_in_track_id_listener_country,
    track_duration_sec_max_3,
    track_duration_sec_share_of_track_id_listener_co,
    track_duration_sec_percent_rank,
    next_listener_birth_year,
    listener_birth_year_vs_source_avg,
    rows_in_track_id_source,
    listener_birth_year_sum_2_prior,
    LENGTH(TRIM(device)) AS device_len,
    CASE WHEN prev_track_duration_sec >= 352 THEN 'A' WHEN prev_track_duration_sec >= 183 THEN 'B' ELSE 'C' END AS prev_track_duration_sec_tier
FROM with_trends
WHERE listener_country = 'UK'
ORDER BY track_duration_sec_share_of_track_id_listener_co NULLS LAST, id
LIMIT 5
