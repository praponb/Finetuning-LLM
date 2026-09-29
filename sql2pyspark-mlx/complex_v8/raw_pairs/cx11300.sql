WITH recent_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped
    FROM plays
    WHERE track_id >= 12
      AND ms_played <= 214343
),
combined_details AS (
    -- Attach listener attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.is_skipped,
        l.signup_date AS listener_signup_date,
        l.birth_year AS listener_birth_year,
        l.username AS listener_username
    FROM recent_plays s
    JOIN listeners l ON l.id = s.listener_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        DENSE_RANK() OVER (ORDER BY ms_played NULLS LAST) AS ms_played_dense_rank,
        ROUND(AVG(listener_birth_year) OVER (PARTITION BY listener_id ORDER BY ms_played, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS listener_birth_year_avg_3,
        LAG(ms_played) OVER w_track_id_by_ms_played AS prev_ms_played,
        DENSE_RANK() OVER (ORDER BY listener_signup_date) AS listener_signup_date_dense_rank,
        SUM(ms_played) OVER (ORDER BY listener_signup_date, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS ms_played_sum_3_prior,
        ROUND(PERCENT_RANK() OVER (ORDER BY listener_signup_date), 3) AS listener_signup_date_percent_rank,
        ROUND(AVG(listener_birth_year) OVER (ORDER BY listener_signup_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS listener_birth_year_avg_7,
        SIZE(COLLECT_SET(track_id) OVER (PARTITION BY listener_id)) AS distinct_track_id_in_listener_id,
        ROUND(AVG(ms_played) OVER (PARTITION BY listener_id ORDER BY ms_played, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS ms_played_avg_7
    FROM combined_details
    WINDOW w_track_id_by_ms_played AS (PARTITION BY track_id ORDER BY ms_played, id)
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        is_skipped,
        ms_played_dense_rank,
        listener_birth_year_avg_3,
        prev_ms_played,
        listener_signup_date_percent_rank,
        distinct_track_id_in_listener_id,
        ms_played_avg_7,
        TO_DATE(played_at) AS played_at_date,
        COALESCE(ms_played_sum_3_prior, 0) AS ms_played_sum_3_prior_filled,
        GREATEST(ms_played, prev_ms_played) AS greatest_ms_played_prev_ms_played
    FROM metrics
    WHERE ms_played < 225733
)
SELECT
    id,
    listener_id,
    played_at,
    ms_played_dense_rank,
    prev_ms_played,
    listener_signup_date_percent_rank,
    distinct_track_id_in_listener_id,
    ms_played_avg_7,
    played_at_date,
    ms_played_sum_3_prior_filled,
    greatest_ms_played_prev_ms_played
FROM labeled
