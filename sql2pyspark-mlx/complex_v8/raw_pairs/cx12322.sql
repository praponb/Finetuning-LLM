WITH cleaned AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        DATE_TRUNC('QUARTER', played_at) AS played_at_quarter,
        CONCAT_WS('|', device, source) AS device_source_label
    FROM plays
    WHERE ms_played >= 120663
      AND source = 'search'
),
combined_details_listener AS (
    -- Attach listener attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.is_skipped,
        s.source,
        s.played_at_quarter,
        s.device_source_label,
        l.signup_date AS listener_signup_date,
        l.birth_year AS listener_birth_year,
        l.interests AS listener_interests
    FROM cleaned s
    LEFT JOIN listeners l ON l.id = s.listener_id
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER w_source_listener_signup_date AS played_at_quarter_dense_rank,
        NTILE(5) OVER (PARTITION BY source, TO_DATE(played_at) ORDER BY listener_signup_date, id) AS listener_signup_date_quintile,
        ROUND(listener_birth_year - AVG(listener_birth_year) OVER (PARTITION BY is_skipped, track_id), 2) AS listener_birth_year_vs_is_skipped_track_id_avg,
        ROUND(CUME_DIST() OVER (ORDER BY played_at_quarter ASC), 3) AS played_at_quarter_cume_dist,
        NTH_VALUE(listener_signup_date, 2) OVER (ORDER BY played_at_quarter ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_listener_signup_date,
        LEAD(played_at_quarter) OVER (PARTITION BY source, TO_DATE(played_at) ORDER BY listener_signup_date, id) AS next_played_at_quarter,
        LEAD(listener_signup_date) OVER (ORDER BY played_at_quarter ASC, id) AS next_listener_signup_date,
        MIN(listener_birth_year) OVER (PARTITION BY MONTH(played_at_quarter), is_skipped) AS played_at_quarter_month_is_skipped_min_listener_,
        ROW_NUMBER() OVER (ORDER BY played_at_quarter ASC, id) AS played_at_quarter_seq
    FROM combined_details_listener
    WINDOW w_source_listener_signup_date AS (PARTITION BY source, MONTH(listener_signup_date) ORDER BY played_at_quarter DESC)
),
top_per_group AS (
    -- Top 5 per group
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        is_skipped,
        source,
        played_at_quarter,
        device_source_label,
        listener_signup_date,
        listener_birth_year,
        listener_interests,
        listener_signup_date_quintile,
        listener_birth_year_vs_is_skipped_track_id_avg,
        played_at_quarter_cume_dist,
        second_listener_signup_date,
        next_played_at_quarter,
        next_listener_signup_date,
        played_at_quarter_month_is_skipped_min_listener_,
        played_at_quarter_seq
    FROM ranked
    WHERE played_at_quarter_dense_rank <= 5
      AND is_skipped = TRUE
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        played_at_quarter,
        device_source_label,
        listener_birth_year,
        listener_interests,
        listener_signup_date_quintile,
        listener_birth_year_vs_is_skipped_track_id_avg,
        played_at_quarter_cume_dist,
        second_listener_signup_date,
        next_listener_signup_date,
        NTH_VALUE(played_at_quarter_seq, 2) OVER (ORDER BY played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at_quarter_seq,
        SUM(listener_birth_year) OVER (PARTITION BY source, listener_signup_date_quintile ORDER BY listener_birth_year_vs_is_skipped_track_id_avg DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS listener_birth_year_sum_2_prior,
        DATEDIFF(listener_signup_date, LAG(listener_signup_date) OVER w_source_listener_signup_date_3) AS days_since_prev_listener_signup_date,
        ROUND(AVG(listener_birth_year) OVER (ORDER BY played_at DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS listener_birth_year_avg_3
    FROM top_per_group
    WINDOW w_source_listener_signup_date_3 AS (PARTITION BY source, listener_signup_date_quintile ORDER BY listener_birth_year_vs_is_skipped_track_id_avg DESC, id)
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    played_at_quarter,
    listener_birth_year,
    listener_interests,
    listener_signup_date_quintile,
    listener_birth_year_vs_is_skipped_track_id_avg,
    played_at_quarter_cume_dist,
    second_listener_signup_date,
    next_listener_signup_date,
    second_played_at_quarter_seq,
    listener_birth_year_sum_2_prior,
    days_since_prev_listener_signup_date,
    listener_birth_year_avg_3,
    LENGTH(TRIM(device_source_label)) AS device_source_label_len,
    DATEDIFF(DATE '2025-12-31', played_at) AS days_since_played_at
FROM with_windows
