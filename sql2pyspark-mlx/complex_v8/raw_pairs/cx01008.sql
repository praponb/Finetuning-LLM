WITH valid_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        is_explicit,
        bpm,
        DATEDIFF(DATE '2025-06-30', release_date) AS days_since_release_date,
        SUBSTRING(TRIM(title), 1, 4) AS title_prefix
    FROM tracks
    WHERE artist_id IS NOT NULL
      AND title IS NOT NULL
),
rollup_base AS (
    -- Group-level totals
    SELECT
        artist_id,
        ROUND(AVG(days_since_release_date), 2) AS avg_days_since_release_date,
        ROUND(SUM(bpm) / NULLIF(SUM(days_since_release_date), 0), 3) AS bpm_to_days_since_release_date_ratio,
        COUNT(*) AS row_count,
        SUM(days_since_release_date) AS total_days_since_release_date
    FROM valid_tracks
    WHERE artist_id IS NOT NULL
    GROUP BY 1
    HAVING COUNT(*) >= 2
),
combined_details_artist AS (
    -- Attach artist attributes
    SELECT s.*, a.is_verified AS artist_is_verified, a.genre AS artist_genre
    FROM rollup_base s
    JOIN artists a ON a.id = s.artist_id
)
SELECT artist_id, field, metric_value
FROM (
    SELECT artist_id, bpm_to_days_since_release_date_ratio, avg_days_since_release_date
    FROM combined_details_artist
) src
UNPIVOT (
    metric_value FOR field IN (bpm_to_days_since_release_date_ratio, avg_days_since_release_date)
) unp
ORDER BY artist_id, field
