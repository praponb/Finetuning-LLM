WITH recent_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        DATE_TRUNC('MONTH', release_date) AS release_date_month,
        DATE_TRUNC('MONTH', release_date) AS release_date_month_2,
        MONTH(release_date) AS release_date_month_3
    FROM tracks
    WHERE (artist_id IS NOT NULL OR title IS NOT NULL)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(duration_sec) OVER (ORDER BY release_date_month, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_duration_sec,
        ROUND((UNIX_TIMESTAMP(release_date_month_2) - UNIX_TIMESTAMP(LAG(release_date_month_2) OVER (ORDER BY release_date_month, id))) / 60, 1) AS minutes_since_prev_release_date_month_2,
        FIRST_VALUE(bpm) IGNORE NULLS OVER (ORDER BY release_date_month, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_bpm,
        MAX(release_date_month_3) OVER (PARTITION BY release_date_month_3, is_explicit) AS release_date_month_3_is_explicit_max_release_dat,
        ROUND(AVG(release_date_month_3) OVER (ORDER BY release_date_month, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS release_date_month_3_avg_2
    FROM recent_tracks
),
screened AS (
    -- Compare against the overall max
    SELECT *
    FROM metrics
    WHERE duration_sec < (SELECT MAX(duration_sec) FROM metrics)
),
with_activity AS (
    -- Compare against the overall max
    SELECT *
    FROM screened
    WHERE first_known_bpm < (SELECT MAX(first_known_bpm) FROM screened)
)
SELECT
    id,
    artist_id,
    title,
    duration_sec,
    release_date,
    is_explicit,
    bpm,
    release_date_month,
    release_date_month_3,
    first_duration_sec,
    minutes_since_prev_release_date_month_2,
    first_known_bpm,
    release_date_month_3_is_explicit_max_release_dat,
    release_date_month_3_avg_2
FROM with_activity
WHERE UPPER(TRIM(title)) LIKE 'E%'
ORDER BY release_date, id
LIMIT 5
