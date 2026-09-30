WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        CONCAT(source, '#', CAST(track_id AS STRING)) AS source_ref
    FROM plays
    WHERE source IS NOT NULL
      AND played_at >= TIMESTAMP '2025-02-06 15:00:00'
),
joined_listener AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.is_skipped,
        s.source_ref,
        l.signup_date AS listener_signup_date,
        l.birth_year AS listener_birth_year,
        l.plan AS listener_plan
    FROM base s
    LEFT JOIN listeners l ON l.id = s.listener_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source_ref,
        listener_signup_date,
        listener_plan,
        listener_birth_year - LAG(listener_birth_year) OVER (PARTITION BY is_skipped, DATE_TRUNC('MONTH', played_at) ORDER BY listener_birth_year NULLS LAST, id) AS listener_birth_year_change,
        DENSE_RANK() OVER (PARTITION BY listener_plan, listener_id ORDER BY listener_signup_date) AS listener_signup_date_dense_rank,
        SUM(ms_played) OVER w_listener_signup_date_month AS listener_signup_date_month_sum_ms_played,
        COUNT(*) OVER (PARTITION BY track_id, listener_id) AS rows_in_track_id_listener_id,
        NTILE(3) OVER (PARTITION BY listener_plan, listener_id ORDER BY listener_signup_date, id) AS listener_signup_date_tercile,
        NTH_VALUE(ms_played, 2) OVER (PARTITION BY listener_plan, listener_id ORDER BY listener_birth_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played,
        SIZE(COLLECT_SET(track_id) OVER (PARTITION BY is_skipped, listener_plan)) AS distinct_track_id_in_is_skipped_listener_plan,
        ROUND(listener_birth_year / NULLIF(SUM(listener_birth_year) OVER w_track_id_listener_id, 0), 4) AS listener_birth_year_share_of_track_id_listener_i,
        SIZE(COLLECT_SET(listener_id) OVER (PARTITION BY is_skipped, listener_plan)) AS distinct_listener_id_in_is_skipped_listener_plan,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', listener_signup_date)) AS rows_in_listener_signup_date_month
    FROM joined_listener
    WINDOW w_listener_signup_date_month AS (PARTITION BY DATE_TRUNC('MONTH', listener_signup_date)), w_track_id_listener_id AS (PARTITION BY track_id, listener_id)
),
top_per_group AS (
    -- Top 2 per group
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source_ref,
        listener_signup_date,
        listener_plan,
        listener_birth_year_change,
        listener_signup_date_month_sum_ms_played,
        rows_in_track_id_listener_id,
        listener_signup_date_tercile,
        second_ms_played,
        distinct_track_id_in_is_skipped_listener_plan,
        listener_birth_year_share_of_track_id_listener_i,
        distinct_listener_id_in_is_skipped_listener_plan,
        rows_in_listener_signup_date_month
    FROM with_trends
    WHERE listener_signup_date_dense_rank <= 2
)
SELECT
    id,
    listener_id,
    track_id,
    ms_played,
    listener_birth_year_change,
    listener_signup_date_month_sum_ms_played,
    second_ms_played,
    distinct_track_id_in_is_skipped_listener_plan,
    listener_birth_year_share_of_track_id_listener_i,
    distinct_listener_id_in_is_skipped_listener_plan,
    rows_in_listener_signup_date_month,
    COALESCE(second_ms_played, 0) AS second_ms_played_filled
FROM top_per_group
