WITH base_artists AS (
    -- Filter artists
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        is_verified,
        CONCAT(artist_name, '#', CAST(debut_year AS STRING)) AS artist_name_ref,
        LOWER(REGEXP_REPLACE(TRIM(artist_name), ' ', '_')) AS artist_name_slug,
        COALESCE(country, 'UNKNOWN') AS country_or_unknown
    FROM artists
),
latest_per_key AS (
    -- Keep the latest row per genre
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        is_verified,
        artist_name_ref,
        artist_name_slug,
        country_or_unknown
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY genre ORDER BY debut_year DESC, id DESC) AS rn
        FROM base_artists
    ) ranked
    WHERE rn = 1
),
deduped AS (
    -- Keep the latest row per genre, country_or_unknown
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        is_verified,
        artist_name_ref,
        artist_name_slug,
        country_or_unknown
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY genre, country_or_unknown ORDER BY debut_year DESC, id DESC) AS row_num
        FROM latest_per_key
    ) ranked
    WHERE row_num = 1
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN debut_year >= 2016 THEN 'HIGH' WHEN debut_year >= 1997 THEN 'MEDIUM' ELSE 'LOW' END AS debut_year_tier,
        CONCAT(artist_name_ref, '#', CAST(debut_year AS STRING)) AS artist_name_ref_ref
    FROM deduped
    WHERE debut_year > 1994
),
line_level_tracks AS (
    -- One row per track
    SELECT
        s.id,
        s.artist_name,
        s.country,
        s.debut_year,
        s.is_verified,
        s.artist_name_ref,
        s.debut_year_tier,
        t.id AS track_id,
        t.release_date AS track_release_date,
        t.title AS track_title,
        t.is_explicit AS track_is_explicit
    FROM derived s
    JOIN tracks t ON t.artist_id = s.id AND LENGTH(TRIM(t.title)) >= 8
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM line_level_tracks
    WHERE debut_year > (SELECT AVG(debut_year) FROM line_level_tracks)
)
SELECT DISTINCT track_title, is_verified, id
FROM screened
