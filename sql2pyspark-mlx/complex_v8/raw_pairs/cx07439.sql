WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        moods,
        ROUND(bpm / NULLIF(duration_sec, 0), 3) AS bpm_per_duration_sec,
        DATEDIFF(DATE '2025-06-30', release_date) AS days_since_release_date,
        LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug
    FROM tracks
    WHERE release_date >= DATE '2024-07-01'
),
per_group AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', release_date) AS release_date_month,
        artist_id,
        ROUND(AVG(duration_sec), 2) AS avg_duration_sec,
        SUM(days_since_release_date) AS total_days_since_release_date,
        COUNT(*) AS row_count,
        MAX(duration_sec) AS max_duration_sec
    FROM filtered
    GROUP BY DATE_TRUNC('MONTH', release_date), artist_id
    HAVING COUNT(*) >= 2
),
with_parent_artist AS (
    -- Enrich with dimension data
    SELECT s.*, a.genre AS artist_genre
    FROM per_group s
    LEFT JOIN artists a ON a.id = s.artist_id AND a.genre IS NOT NULL
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(row_count) OVER (PARTITION BY artist_genre, artist_id ORDER BY max_duration_sec DESC, release_date_month, artist_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_row_count,
        MIN(row_count) OVER (PARTITION BY artist_genre, artist_id ORDER BY max_duration_sec DESC, release_date_month, artist_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_row_count_so_far,
        LAG(avg_duration_sec, 2) OVER (PARTITION BY artist_genre, artist_id ORDER BY max_duration_sec DESC, release_date_month, artist_id) AS prev2_avg_duration_sec
    FROM with_parent_artist
),
filtered_2 AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND t.bpm > 94
        )
),
screened AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM filtered_2 s
    WHERE s.artist_id IN (
            SELECT t.artist_id
            FROM tracks t
            WHERE t.bpm >= 96
              AND t.release_date >= DATE '2024-11-06'
        )
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT row_count, release_date_month, artist_genre, avg_duration_sec
    FROM screened
) src
PIVOT (
    SUM(avg_duration_sec)
    FOR artist_genre IN ('Indie', 'Pop', 'Jazz')
)
