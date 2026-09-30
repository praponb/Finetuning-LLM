WITH prepared_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        bpm,
        LENGTH(TRIM(title)) AS title_len,
        LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug
    FROM tracks
    WHERE bpm > 94
      AND artist_id >= 11
),
latest_per_key AS (
    -- Keep the latest row per artist_id
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        bpm,
        title_len,
        title_slug
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY duration_sec DESC, id DESC) AS rn
        FROM prepared_tracks
    ) ranked
    WHERE rn = 1
),
screened AS (
    -- Keep rows whose artist matches
    SELECT s.*
    FROM latest_per_key s
    WHERE EXISTS (
            SELECT 1
            FROM artists a
            WHERE a.id = s.artist_id
              AND a.debut_year > 1997
              AND a.is_verified = TRUE
        )
),
with_activity AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM screened s
    WHERE s.artist_id IN (
            SELECT t.artist_id
            FROM tracks t
        )
)
-- Final projection
SELECT
    id,
    artist_id,
    duration_sec,
    is_explicit,
    title_len,
    title_slug,
    bpm + title_len AS bpm_plus_title_len
FROM with_activity
WHERE bpm > 75
ORDER BY title_len
