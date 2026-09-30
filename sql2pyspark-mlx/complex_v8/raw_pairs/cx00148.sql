WITH filtered AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        moods,
        ARRAY_JOIN(ARRAY_SORT(moods), ',') AS moods_text
    FROM tracks
    WHERE bpm <= 163
)
SELECT id, moods_text AS category, release_date AS day
FROM filtered
WHERE ARRAY_CONTAINS(moods, 'sad')
UNION ALL
SELECT birth_year AS id, username AS category, signup_date AS day
FROM listeners
WHERE country = 'KR'
