WITH cleaned_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        bpm,
        moods
    FROM tracks
    WHERE SIZE(moods) >= 2
      AND LOWER(title) LIKE '%w %'
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        ABS(duration_sec - 279) AS duration_sec_distance,
        DATE_TRUNC('YEAR', release_date) AS release_date_year
    FROM cleaned_tracks
    WHERE artist_id IS NOT NULL
),
flattened AS (
    -- One row per mood
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.bpm,
        s.duration_sec_distance,
        s.release_date_year,
        t.mood
    FROM annotated s
    LATERAL VIEW EXPLODE(s.moods) t AS mood
),
with_details AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.bpm,
        s.release_date_year,
        s.mood,
        a.debut_year AS artist_debut_year
    FROM flattened s
    LEFT JOIN artists a ON a.id = s.artist_id
),
joined_artist AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.bpm,
        s.release_date_year,
        s.mood,
        s.artist_debut_year,
        a.artist_name,
        a.is_verified AS artist_is_verified
    FROM with_details s
    JOIN artists a ON a.id = s.artist_id
)
SELECT
    id,
    artist_id,
    duration_sec,
    release_date,
    bpm,
    release_date_year,
    mood,
    artist_name,
    INITCAP(TRIM(artist_name)) AS artist_name_clean
FROM joined_artist
ORDER BY artist_id DESC, id, mood
LIMIT 5
