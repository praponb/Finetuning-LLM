WITH valid_tracks AS (
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
    WHERE LOWER(title) LIKE '%li%'
),
without_matches AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM valid_tracks s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND SIZE(t.moods) >= 1
              AND t.is_explicit = FALSE
        )
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        DATE_TRUNC('YEAR', release_date) AS release_date_year
    FROM without_matches
),
filtered AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM derived s
    WHERE EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
        )
      AND s.is_explicit = TRUE
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        artist_id,
        title,
        release_date,
        bpm,
        release_date_year + INTERVAL 30 MINUTES AS release_date_year_plus_30m,
        bpm + duration_sec AS bpm_plus_duration_sec,
        LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug
    FROM filtered
)
SELECT
    id,
    artist_id,
    title,
    release_date,
    release_date_year_plus_30m,
    bpm_plus_duration_sec,
    CASE WHEN bpm >= 156 THEN 'HIGH' WHEN bpm >= 96 THEN 'MEDIUM' ELSE 'LOW' END AS bpm_tier
FROM tagged
WHERE release_date BETWEEN DATE '2024-04-29' AND DATE '2025-08-02'
ORDER BY id DESC, bpm_plus_duration_sec DESC NULLS FIRST
