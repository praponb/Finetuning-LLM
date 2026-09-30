WITH recent AS (
    -- Scope the artists rows
    SELECT
        id,
        artist_name,
        country,
        is_verified
    FROM artists
    WHERE country IN ('TH', 'BR')
      AND debut_year BETWEEN 1991 AND 2018
),
filtered AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM recent s
    WHERE s.id IN (
            SELECT t.artist_id
            FROM tracks t
        )
),
without_matches AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.id
        )
      AND s.artist_name IS NOT NULL
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        artist_name,
        is_verified,
        CONCAT_WS(': ', country, artist_name) AS country_artist_name_label,
        CASE WHEN is_verified THEN 1 ELSE 0 END AS is_verified_flag
    FROM without_matches
)
SELECT
    id,
    country_artist_name_label,
    SUBSTRING(TRIM(artist_name), 1, 2) AS artist_name_prefix
FROM tagged
WHERE UPPER(TRIM(artist_name)) LIKE 'P%'
ORDER BY id
