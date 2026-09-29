WITH cleaned AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        moods
    FROM tracks
    WHERE is_explicit = FALSE
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', release_date) AS release_date_month,
        COUNT(*) AS cnt,
        ROUND(AVG(duration_sec), 2) AS avg_duration_sec
    FROM cleaned
    GROUP BY DATE_TRUNC('MONTH', release_date)
),
screened AS (
    -- Compare against the overall min
    SELECT *
    FROM grouped
    WHERE cnt > (SELECT MIN(cnt) FROM grouped)
)
-- Final projection
SELECT
    release_date_month,
    cnt,
    avg_duration_sec,
    DATE_TRUNC('MONTH', release_date_month) AS release_date_month_month
FROM screened
ORDER BY avg_duration_sec
