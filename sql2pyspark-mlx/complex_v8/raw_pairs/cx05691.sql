WITH scoped AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        CONCAT(title, '#', CAST(id AS STRING)) AS title_ref,
        YEAR(release_date) AS release_date_year,
        SUBSTRING(TRIM(title), 1, 3) AS title_prefix
    FROM tracks
    WHERE (LENGTH(TRIM(title)) >= 5 OR NOT is_explicit)
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MIN(p.played_at) FROM plays p WHERE p.track_id = s.id) AS first_played_at
    FROM scoped s
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        bpm,
        moods,
        title_ref,
        title_prefix,
        first_played_at,
        CONCAT(title, '#', CAST(id AS STRING)) AS title_ref_2,
        IF(is_explicit, 'yes', 'no') AS is_explicit_label
    FROM profiled
)
-- Final projection
SELECT
    id,
    artist_id,
    title,
    is_explicit,
    bpm,
    moods,
    title_prefix,
    first_played_at,
    title_ref_2,
    is_explicit_label
FROM annotated
WHERE first_played_at < TIMESTAMP '2025-02-17 09:00:00'
ORDER BY title DESC, first_played_at DESC, id
LIMIT 5
