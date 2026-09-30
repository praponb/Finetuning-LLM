WITH filtered_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods
    FROM tracks
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(bpm / NULLIF(duration_sec, 0), 3) AS bpm_per_duration_sec,
        LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug
    FROM filtered_tracks
),
scored_2 AS (
    -- Label and score rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        moods,
        bpm_per_duration_sec,
        DATEDIFF(DATE '2026-01-01', release_date) AS days_since_release_date,
        COALESCE(bpm, 0) AS bpm_filled
    FROM scored
    WHERE title IS NOT NULL
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(bpm_per_duration_sec / NULLIF(SUM(bpm_per_duration_sec) OVER (PARTITION BY artist_id), 0), 4) AS bpm_per_duration_sec_share_of_artist_id,
        FIRST_VALUE(duration_sec) OVER (PARTITION BY artist_id ORDER BY bpm_filled DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_duration_sec,
        MIN(days_since_release_date) OVER (PARTITION BY artist_id) AS artist_id_min_days_since_release_date,
        LAG(days_since_release_date, 3) OVER (ORDER BY bpm_per_duration_sec DESC, id) AS prev3_days_since_release_date
    FROM scored_2
)
SELECT
    id,
    title,
    duration_sec,
    moods,
    bpm_per_duration_sec,
    prev3_days_since_release_date
FROM ranked
ORDER BY duration_sec DESC
