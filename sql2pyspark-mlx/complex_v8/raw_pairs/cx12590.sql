WITH filtered_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        is_skipped,
        source,
        DATEDIFF(DATE '2026-01-01', played_at) AS days_since_played_at
    FROM plays
    WHERE NOT is_skipped
),
with_details_listener AS (
    -- Attach listener attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.is_skipped,
        s.source,
        l.interests AS listener_interests,
        l.signup_date AS listener_signup_date,
        t.bpm AS track_bpm
    FROM filtered_plays s
    JOIN listeners l ON l.id = s.listener_id
    JOIN tracks t ON t.id = s.track_id
    WHERE ARRAY_CONTAINS(l.interests, 'live')
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        track_id,
        played_at,
        is_skipped,
        listener_interests,
        LAG(listener_signup_date) OVER (PARTITION BY source ORDER BY track_bpm, id) AS prev_listener_signup_date,
        ROUND(AVG(track_bpm) OVER (PARTITION BY listener_id, source), 2) AS listener_id_source_avg_track_bpm,
        ROUND(track_bpm - AVG(track_bpm) OVER (PARTITION BY is_skipped, YEAR(played_at)), 2) AS track_bpm_vs_is_skipped_played_at_year_avg,
        ROUND(track_bpm / NULLIF(SUM(track_bpm) OVER (PARTITION BY is_skipped, YEAR(played_at)), 0), 4) AS track_bpm_share_of_is_skipped_played_at_year,
        COUNT(*) OVER (PARTITION BY is_skipped, YEAR(played_at)) AS rows_in_is_skipped_played_at_year,
        ROUND(track_bpm - AVG(track_bpm) OVER w_listener_id_source, 2) AS track_bpm_vs_listener_id_source_avg,
        track_bpm - LAG(track_bpm) OVER (ORDER BY listener_signup_date, id) AS track_bpm_change,
        MAX(track_bpm) OVER (PARTITION BY YEAR(played_at) ORDER BY listener_signup_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_track_bpm_so_far,
        FIRST_VALUE(listener_signup_date) OVER (PARTITION BY YEAR(played_at) ORDER BY listener_signup_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_listener_signup_date,
        SUM(track_bpm) OVER (PARTITION BY listener_id, source) AS listener_id_source_sum_track_bpm
    FROM with_details_listener
    WINDOW w_listener_id_source AS (PARTITION BY listener_id, source)
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN max_track_bpm_so_far >= 1452 THEN 'large' WHEN max_track_bpm_so_far >= 408 THEN 'medium' ELSE 'small' END AS max_track_bpm_so_far_tier,
        track_bpm_change + max_track_bpm_so_far AS track_bpm_change_plus_max_track_bpm_so_far,
        COALESCE(track_bpm_change, 0) AS track_bpm_change_filled
    FROM ranked
    WHERE rows_in_is_skipped_played_at_year > 3
),
grouped AS (
    -- Summarise per group
    SELECT
        TO_DATE(played_at) AS played_at_day,
        DATE_TRUNC('MONTH', first_listener_signup_date) AS first_listener_signup_date_month,
        track_id,
        ROUND(SUM(track_bpm_share_of_is_skipped_played_at_year), 2) AS total_track_bpm_share_of_is_skipped_played_at_ye,
        MAX(played_at) AS last_played_at,
        SUM(CASE WHEN NOT is_skipped THEN listener_id_source_sum_track_bpm ELSE 0 END) AS listener_id_source_sum_track_bpm_not_is_skipped
    FROM derived
    GROUP BY TO_DATE(played_at), DATE_TRUNC('MONTH', first_listener_signup_date), track_id
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        DATEDIFF(played_at_day, LAG(played_at_day) OVER (ORDER BY last_played_at ASC, played_at_day, first_listener_signup_date_month, track_id)) AS days_since_prev_played_at_day
    FROM grouped
)
-- Final projection
SELECT
    played_at_day,
    total_track_bpm_share_of_is_skipped_played_at_ye,
    last_played_at,
    listener_id_source_sum_track_bpm_not_is_skipped,
    TO_DATE(last_played_at) AS last_played_at_date,
    DATEDIFF(DATE '2026-01-01', last_played_at) AS days_since_last_played_at
FROM with_trends
WHERE track_id IS NOT NULL
ORDER BY listener_id_source_sum_track_bpm_not_is_skipped
