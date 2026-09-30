WITH cleaned_tracks AS (
    -- Filter tracks
    SELECT *
    FROM tracks
    WHERE is_explicit = FALSE
),
with_details_artist AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.moods,
        a.debut_year AS artist_debut_year
    FROM cleaned_tracks s
    JOIN artists a ON a.id = s.artist_id
    WHERE a.debut_year < 2018
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        SUBSTRING(TRIM(title), 1, 4) AS title_prefix,
        DATEDIFF(DATE '2025-06-30', release_date) AS days_since_release_date,
        LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug
    FROM with_details_artist
),
enriched_artist AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.artist_debut_year,
        s.title_prefix,
        s.days_since_release_date,
        s.title_slug,
        a.genre AS artist_genre,
        a.debut_year AS artist_debut_year_2,
        a.artist_name
    FROM enriched s
    LEFT JOIN artists a ON a.id = s.artist_id
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        artist_debut_year,
        title_prefix,
        days_since_release_date,
        artist_genre,
        artist_debut_year_2,
        CONCAT_WS('|', title, title_slug) AS title_title_slug_label,
        DATE_ADD(release_date, 365) AS release_date_plus_365d
    FROM enriched_artist
),
enriched_2 AS (
    -- Add calculated columns
    SELECT
        *,
        DATE_TRUNC('MONTH', release_date) AS release_date_month,
        ROUND(artist_debut_year / NULLIF(artist_debut_year_2, 0), 3) AS artist_debut_year_per_artist_debut_year_2,
        LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug
    FROM tagged
)
SELECT
    id,
    title,
    release_date,
    artist_debut_year,
    title_prefix,
    days_since_release_date,
    artist_genre,
    title_title_slug_label,
    release_date_month,
    artist_debut_year_per_artist_debut_year_2,
    title_slug
FROM enriched_2
