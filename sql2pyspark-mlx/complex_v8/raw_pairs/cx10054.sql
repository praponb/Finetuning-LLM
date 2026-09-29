WITH scoped_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        moods
    FROM tracks
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        DATEDIFF(DATE '2026-01-01', release_date) AS days_since_release_date,
        COALESCE(duration_sec, 0) AS duration_sec_filled,
        SIZE(moods) AS moods_count
    FROM scoped_tracks
),
screened AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM tagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND ARRAY_CONTAINS(t.moods, 'chill')
        )
      AND NOT s.is_explicit
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        moods,
        days_since_release_date,
        duration_sec_filled,
        moods_count,
        COALESCE(duration_sec, 0) AS duration_sec_filled_2,
        LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug,
        ARRAY_CONTAINS(moods, 'sad') AS has_sad
    FROM screened
    WHERE duration_sec >= 164
)
SELECT
    is_explicit,
    moods_count,
    GROUPING_ID() AS grouping_level,
    COUNT(*) AS cnt
FROM scored
GROUP BY CUBE(is_explicit, moods_count)
ORDER BY is_explicit NULLS LAST, moods_count NULLS LAST
