WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        DATE_TRUNC('MONTH', release_date) AS release_date_month
    FROM tracks
    WHERE bpm >= 104
      AND LOWER(title) LIKE '%pe%'
),
stacked AS (
    -- Stack the branches
    SELECT title, release_date, 'track' AS record_type
    FROM recent
    WHERE LENGTH(TRIM(title)) >= 8
      AND release_date BETWEEN DATE '2024-04-21' AND DATE '2025-04-02'
    UNION ALL
    SELECT username, signup_date, 'listener' AS record_type
    FROM listeners
)
SELECT record_type, COUNT(*) AS n_rows, MAX(release_date) AS latest_release_date
FROM stacked
GROUP BY record_type
ORDER BY record_type
