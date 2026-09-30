WITH prepared_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        LOWER(TRIM(title)) AS title_clean
    FROM tracks
    WHERE bpm >= 75
),
deduped AS (
    -- Keep the latest row per artist_id
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        title_clean
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY release_date DESC, id DESC) AS recency_rank
        FROM prepared_tracks
    ) ranked
    WHERE recency_rank = 1
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(duration_sec) OVER w_release_date_year_is_explici AS release_date_year_is_explicit_max_duration_sec,
        NTILE(3) OVER (ORDER BY bpm, id) AS bpm_tercile,
        ROW_NUMBER() OVER (ORDER BY bpm, id) AS rn,
        DENSE_RANK() OVER (ORDER BY bpm) AS bpm_dense_rank
    FROM deduped
    WINDOW w_release_date_year_is_explici AS (PARTITION BY YEAR(release_date), is_explicit)
)
-- Subtotals with GROUPING SETS
SELECT
    is_explicit,
    artist_id,
    MIN(rn) AS min_rn
FROM ranked
GROUP BY GROUPING SETS ((is_explicit, artist_id), (is_explicit), (artist_id))
ORDER BY is_explicit NULLS LAST, artist_id NULLS LAST
