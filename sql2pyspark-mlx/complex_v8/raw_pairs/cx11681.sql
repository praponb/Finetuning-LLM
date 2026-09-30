WITH recent_artists AS (
    -- Scope the artists rows
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        is_verified,
        LOWER(TRIM(country)) AS country_clean,
        CONCAT(artist_name, '#', CAST(debut_year AS STRING)) AS artist_name_ref,
        CASE WHEN is_verified THEN 1 ELSE 0 END AS is_verified_flag
    FROM artists
    WHERE (NOT is_verified OR country IN ('KR', 'TH', 'JP'))
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        genre,
        debut_year,
        is_verified,
        country_clean,
        artist_name_ref,
        is_verified_flag,
        is_verified_flag - LAG(is_verified_flag) OVER (ORDER BY debut_year, id) AS is_verified_flag_change,
        COUNT(*) OVER (PARTITION BY is_verified_flag, genre) AS rows_in_is_verified_flag_genre
    FROM recent_artists
    WINDOW w_is_verified_flag_genre AS (PARTITION BY is_verified_flag, genre)
),
with_stats AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        (SELECT MIN(t.duration_sec) FROM tracks t WHERE t.artist_id = s.id) AS min_track_duration_sec
    FROM with_trends s
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM with_stats
    WHERE rows_in_is_verified_flag_genre > (SELECT AVG(rows_in_is_verified_flag_genre) FROM with_stats)
),
unique_rows AS (
    -- Keep the latest row per genre
    SELECT
        id,
        genre,
        debut_year,
        is_verified,
        country_clean,
        artist_name_ref,
        is_verified_flag,
        is_verified_flag_change,
        rows_in_is_verified_flag_genre,
        min_track_duration_sec
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY genre ORDER BY is_verified_flag DESC, id DESC) AS rn
        FROM without_matches
    ) ranked
    WHERE rn = 1
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        LENGTH(TRIM(artist_name_ref)) AS artist_name_ref_len,
        GREATEST(min_track_duration_sec, debut_year) AS greatest_min_track_duration_sec_debut_year
    FROM unique_rows
)
SELECT DISTINCT is_verified
FROM scored
