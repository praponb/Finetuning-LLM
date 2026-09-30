WITH valid_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        SUBSTRING(TRIM(title), 1, 2) AS title_prefix,
        DATE_FORMAT(release_date, 'yyyy-MM') AS release_date_label
    FROM tracks
    WHERE is_explicit = FALSE
),
with_details_artist AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.release_date_label,
        a.country AS artist_country
    FROM valid_tracks s
    JOIN artists a ON a.id = s.artist_id AND a.genre <> 'Pop'
    WHERE a.country NOT IN ('JP', 'TH')
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.release_date_label,
        s.artist_country,
        a.debut_year AS artist_debut_year
    FROM with_details_artist s
    JOIN artists a ON a.id = s.artist_id AND a.is_verified = TRUE
    WHERE a.debut_year < 2006
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(p.ms_played), 2) FROM plays p WHERE p.track_id = s.id) AS avg_play_ms_played
    FROM enriched s
),
enriched_2 AS (
    -- Derive helper columns
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(release_date_label), ' ', '_')) AS release_date_label_slug,
        COALESCE(artist_debut_year, 0) AS artist_debut_year_filled,
        SUBSTRING(TRIM(release_date_label), 1, 4) AS release_date_label_prefix
    FROM profiled
    WHERE artist_id <= 14
),
with_activity AS (
    -- Keep rows whose artist matches
    SELECT s.*
    FROM enriched_2 s
    WHERE EXISTS (
            SELECT 1
            FROM artists a
            WHERE a.id = s.artist_id
              AND a.country IS NOT NULL
              AND a.is_verified = TRUE
        )
)
SELECT
    id,
    artist_id,
    artist_country,
    avg_play_ms_played,
    release_date_label_slug,
    release_date_label_prefix
FROM with_activity
WHERE duration_sec <= 312
ORDER BY avg_play_ms_played DESC, id NULLS LAST
LIMIT 10
