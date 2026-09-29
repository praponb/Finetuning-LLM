WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        moods,
        FLOOR(bpm / 25) * 25 AS bpm_bucket,
        SUBSTRING(TRIM(title), 1, 2) AS title_prefix,
        bpm + duration_sec AS bpm_plus_duration_sec
    FROM tracks
),
enriched AS (
    -- Add calculated columns
    SELECT *, ABS(bpm_plus_duration_sec - 357) AS bpm_plus_duration_sec_distance
    FROM valid
    WHERE LOWER(title) LIKE '%ur%'
),
with_parent_artist AS (
    -- Attach artist attributes
    SELECT s.*, a.country AS artist_country
    FROM enriched s
    JOIN artists a ON a.id = s.artist_id
    WHERE a.country IN ('BR', 'JP', 'TH')
),
with_activity AS (
    -- Keep rows whose artist matches
    SELECT s.*
    FROM with_parent_artist s
    WHERE EXISTS (
            SELECT 1
            FROM artists a
            WHERE a.id = s.artist_id
              AND a.country IN ('JP', 'TH')
        )
)
SELECT
    id,
    artist_id,
    title,
    duration_sec,
    release_date,
    is_explicit,
    moods,
    bpm_bucket,
    title_prefix,
    bpm_plus_duration_sec,
    artist_country,
    FLOOR(bpm_plus_duration_sec / 100) * 100 AS bpm_plus_duration_sec_bucket,
    IF(is_explicit, 'yes', 'no') AS is_explicit_label
FROM with_activity
WHERE bpm_plus_duration_sec_distance BETWEEN 180 AND 510
ORDER BY release_date, id
LIMIT 20
