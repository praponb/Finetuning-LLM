WITH cleaned_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        DATE_TRUNC('MONTH', release_date) AS release_date_month,
        DATEDIFF(DATE '2025-06-30', release_date) AS days_since_release_date
    FROM tracks
    WHERE UPPER(TRIM(title)) LIKE 'R%'
),
deduped AS (
    -- Keep the latest row per artist_id
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        release_date_month,
        days_since_release_date
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY bpm DESC, id DESC) AS recency_rank
        FROM cleaned_tracks
    ) ranked
    WHERE recency_rank = 1
)
SELECT
    is_explicit,
    artist_id,
    PERCENTILE_APPROX(days_since_release_date, 0.5) AS median_days_since_release_date,
    MIN(duration_sec) AS min_duration_sec,
    DATEDIFF(MAX(release_date), MIN(release_date)) AS release_date_span_days
FROM deduped
GROUP BY GROUPING SETS ((is_explicit, artist_id), (is_explicit))
ORDER BY is_explicit NULLS LAST, artist_id NULLS LAST
