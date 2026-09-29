WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        moods,
        ARRAY_JOIN(ARRAY_SORT(moods), ',') AS moods_text,
        DATE_FORMAT(release_date, 'MMM yyyy') AS release_date_label,
        LOWER(TRIM(title)) AS title_clean
    FROM tracks
    WHERE (NOT is_explicit OR duration_sec BETWEEN 142 AND 352)
),
with_details_artist AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.moods,
        s.release_date_label,
        s.title_clean,
        a.debut_year AS artist_debut_year
    FROM scoped s
    LEFT JOIN artists a ON a.id = s.artist_id
),
with_details_artist_2 AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.moods,
        s.title_clean,
        s.artist_debut_year,
        a.debut_year AS artist_debut_year_2,
        a.artist_name
    FROM with_details_artist s
    LEFT JOIN artists a ON a.id = s.artist_id
),
summary AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', release_date) AS release_date_month,
        artist_id,
        COUNT(*) AS n_rows,
        MAX(release_date) AS last_release_date
    FROM with_details_artist_2
    GROUP BY 1, 2
),
scored AS (
    -- Derive helper columns
    SELECT
        release_date_month,
        artist_id,
        n_rows,
        ABS(n_rows - 7) AS n_rows_distance,
        TO_DATE(release_date_month) AS release_date_month_date
    FROM summary
    WHERE n_rows <= 8
),
with_parent_artist AS (
    -- Attach artist attributes
    SELECT
        s.release_date_month,
        s.artist_id,
        s.n_rows,
        s.n_rows_distance,
        a.is_verified AS artist_is_verified,
        a.artist_name,
        a.debut_year AS artist_debut_year
    FROM scored s
    LEFT JOIN artists a ON a.id = s.artist_id
)
SELECT
    release_date_month,
    artist_id,
    n_rows,
    artist_is_verified,
    n_rows - artist_debut_year AS n_rows_minus_artist_debut_year
FROM with_parent_artist
ORDER BY artist_id, n_rows DESC NULLS FIRST, release_date_month
LIMIT 10
