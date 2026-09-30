WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        release_date,
        bpm,
        moods,
        DATEDIFF(DATE '2026-01-01', release_date) AS days_since_release_date
    FROM tracks
    WHERE SIZE(moods) >= 2
      AND is_explicit = TRUE
),
profiled AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(t.bpm), 2) FROM tracks t WHERE t.artist_id = s.artist_id) AS avg_track_bpm
    FROM filtered s
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        release_date,
        bpm,
        moods,
        days_since_release_date,
        avg_track_bpm,
        COALESCE(bpm, 0) AS bpm_filled,
        MONTH(release_date) AS release_date_month,
        DATE_FORMAT(release_date, 'MMM yyyy') AS release_date_label
    FROM profiled
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', release_date) AS release_date_month_2,
        release_date_month,
        MIN(release_date) AS first_release_date,
        DATEDIFF(MAX(release_date), MIN(release_date)) AS release_date_span_days,
        SUM(days_since_release_date) AS total_days_since_release_date,
        MIN(bpm_filled) AS min_bpm_filled,
        ROUND(AVG(bpm_filled), 2) AS avg_bpm_filled
    FROM derived
    GROUP BY 1, 2
)
SELECT
    release_date_month_2,
    release_date_month,
    first_release_date,
    total_days_since_release_date,
    min_bpm_filled,
    avg_bpm_filled,
    DATEDIFF(DATE '2025-06-30', first_release_date) AS days_since_first_release_date
FROM summary
ORDER BY avg_bpm_filled DESC, min_bpm_filled, release_date_month_2, release_date_month
LIMIT 20
