WITH recent_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        QUARTER(release_date) AS release_date_quarter,
        IF(is_explicit, 'yes', 'no') AS is_explicit_label
    FROM tracks
    WHERE YEAR(release_date) = 2024
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        MONTH(release_date) AS release_date_month,
        CONCAT(is_explicit_label, '#', CAST(release_date_quarter AS STRING)) AS is_explicit_label_ref
    FROM recent_tracks
),
with_details_artist AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.release_date_quarter,
        s.is_explicit_label,
        s.release_date_month,
        s.is_explicit_label_ref,
        a.country AS artist_country,
        a.artist_name
    FROM scored s
    JOIN artists a ON a.id = s.artist_id
),
stats AS (
    -- Summarise per group
    SELECT
        release_date_month,
        artist_country,
        is_explicit,
        MIN(release_date) AS first_release_date,
        COUNT(*) AS cnt,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) FILTER (WHERE LENGTH(TRIM(artist_name)) >= 8) AS n_length_trim_artist_name_8,
        MAX(release_date) AS last_release_date
    FROM with_details_artist
    GROUP BY release_date_month, artist_country, is_explicit
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM stats
    WHERE distinct_ids > (SELECT AVG(distinct_ids) FROM stats)
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM screened
    WHERE n_length_trim_artist_name_8 > (SELECT AVG(n_length_trim_artist_name_8) FROM screened)
)
-- Final projection
SELECT
    release_date_month,
    artist_country,
    is_explicit,
    first_release_date,
    cnt,
    distinct_ids,
    last_release_date,
    COALESCE(artist_country, 'UNKNOWN') AS artist_country_or_unknown
FROM without_matches
WHERE first_release_date < DATE '2025-08-07'
