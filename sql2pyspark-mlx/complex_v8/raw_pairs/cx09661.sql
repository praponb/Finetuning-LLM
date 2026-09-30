WITH scoped AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        duration_sec,
        bpm,
        moods,
        LOWER(TRIM(title)) AS title_clean
    FROM tracks
    WHERE duration_sec BETWEEN 183 AND 352
      AND NOT ARRAY_CONTAINS(moods, 'sad')
),
unique_rows AS (
    -- Keep the latest row per artist_id
    SELECT
        id,
        artist_id,
        duration_sec,
        bpm,
        moods,
        title_clean
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY bpm DESC, id DESC) AS rn
        FROM scoped
    ) ranked
    WHERE rn = 1
)
SELECT
    id,
    artist_id,
    duration_sec,
    bpm,
    moods,
    SUBSTRING(TRIM(title_clean), 1, 2) AS title_clean_prefix
FROM unique_rows
WHERE artist_id <= 7
ORDER BY duration_sec, id NULLS LAST
