WITH recent AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source,
        YEAR(played_at) AS played_at_year,
        ABS(ms_played - 178909) AS ms_played_distance
    FROM plays
    WHERE (track_id IS NOT NULL OR ms_played >= 151112)
),
joined_listener AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device,
        s.source,
        s.played_at_year,
        s.ms_played_distance,
        l.plan AS listener_plan,
        l.signup_date AS listener_signup_date,
        l.birth_year AS listener_birth_year
    FROM recent s
    LEFT JOIN listeners l ON l.id = s.listener_id
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(ms_played_distance - AVG(ms_played_distance) OVER w_listener_signup_date_month, 2) AS ms_played_distance_vs_listener_signup_date_month,
        MAX(played_at_year) OVER (PARTITION BY source) AS source_max_played_at_year,
        ROUND(played_at_year / NULLIF(SUM(played_at_year) OVER (PARTITION BY DATE_TRUNC('MONTH', listener_signup_date)), 0), 4) AS played_at_year_share_of_listener_signup_date_mon,
        LAST_VALUE(played_at_year) IGNORE NULLS OVER (ORDER BY listener_birth_year ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_played_at_year,
        MIN(ms_played_distance) OVER (PARTITION BY source) AS source_min_ms_played_distance,
        ROUND(PERCENT_RANK() OVER w_all_by_played_at_year_ties, 3) AS played_at_year_percent_rank,
        DENSE_RANK() OVER (PARTITION BY listener_id ORDER BY listener_birth_year DESC) AS listener_birth_year_dense_rank,
        DATEDIFF(listener_signup_date, LAG(listener_signup_date) OVER (ORDER BY played_at ASC, id)) AS days_since_prev_listener_signup_date,
        ROUND(PERCENT_RANK() OVER w_all_by_played_at_ties, 3) AS played_at_percent_rank,
        ROUND(listener_birth_year / NULLIF(SUM(listener_birth_year) OVER w_listener_signup_date_month, 0), 4) AS listener_birth_year_share_of_listener_signup_dat,
        RANK() OVER (PARTITION BY listener_id ORDER BY listener_birth_year DESC) AS listener_birth_year_rank,
        LAG(listener_birth_year, 3) OVER (ORDER BY played_at ASC, id) AS prev3_listener_birth_year,
        ROUND(PERCENT_RANK() OVER (PARTITION BY listener_id ORDER BY listener_birth_year DESC, id), 3) AS listener_birth_year_percent_rank,
        SUM(ms_played_distance) OVER (ORDER BY listener_birth_year ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ms_played_distance
    FROM joined_listener
    WINDOW w_listener_signup_date_month AS (PARTITION BY DATE_TRUNC('MONTH', listener_signup_date)), w_all_by_played_at_year_ties AS (ORDER BY played_at_year ASC NULLS LAST), w_all_by_played_at_ties AS (ORDER BY played_at ASC)
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        track_id,
        device,
        played_at_year,
        ms_played_distance,
        listener_plan,
        listener_signup_date,
        listener_birth_year,
        ms_played_distance_vs_listener_signup_date_month,
        played_at_year_share_of_listener_signup_date_mon,
        last_known_played_at_year,
        source_min_ms_played_distance,
        listener_birth_year_dense_rank,
        days_since_prev_listener_signup_date,
        listener_birth_year_share_of_listener_signup_dat,
        listener_birth_year_rank,
        listener_birth_year_percent_rank,
        QUARTER(listener_signup_date) AS listener_signup_date_quarter,
        DATE_ADD(listener_signup_date, 365) AS listener_signup_date_plus_365d
    FROM with_windows
    WHERE last_known_played_at_year <= 2025
)
SELECT
    id,
    track_id,
    device,
    played_at_year,
    ms_played_distance,
    listener_plan,
    listener_birth_year,
    ms_played_distance_vs_listener_signup_date_month,
    played_at_year_share_of_listener_signup_date_mon,
    source_min_ms_played_distance,
    listener_birth_year_dense_rank,
    days_since_prev_listener_signup_date,
    listener_birth_year_share_of_listener_signup_dat,
    listener_birth_year_percent_rank,
    listener_signup_date_quarter,
    listener_signup_date_plus_365d,
    LOWER(TRIM(device)) AS device_clean,
    FLOOR(listener_birth_year / 10) * 10 AS listener_birth_year_bucket
FROM scored
