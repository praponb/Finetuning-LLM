WITH prepared AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        moods,
        UPPER(TRIM(title)) AS title_clean,
        DATE_TRUNC('YEAR', release_date) AS release_date_year
    FROM tracks
    WHERE duration_sec BETWEEN 165 AND 409
),
joined_artist AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.is_explicit,
        s.moods,
        s.title_clean,
        s.release_date_year,
        a.country AS artist_country,
        a.is_verified AS artist_is_verified,
        a.debut_year AS artist_debut_year
    FROM prepared s
    JOIN artists a ON a.id = s.artist_id
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT_WS(': ', title_clean, artist_country) AS title_clean_artist_country_label
    FROM joined_artist
    WHERE NOT ARRAY_CONTAINS(moods, 'sad')
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        LAST_VALUE(release_date_year) OVER (PARTITION BY is_explicit ORDER BY artist_debut_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_release_date_year,
        COUNT(*) OVER (PARTITION BY is_explicit ORDER BY artist_debut_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        FIRST_VALUE(artist_debut_year) OVER (PARTITION BY is_explicit ORDER BY artist_debut_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_artist_debut_year
    FROM tagged
),
screened AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM ranked s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
        )
)
SELECT
    id,
    is_explicit,
    title_clean,
    release_date_year,
    artist_is_verified,
    artist_debut_year,
    title_clean_artist_country_label,
    first_artist_debut_year
FROM screened
ORDER BY title_clean_artist_country_label DESC
