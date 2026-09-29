WITH base_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods
    FROM tracks
    WHERE duration_sec >= 209
      AND NOT is_explicit
),
artist_id_stats AS (
    -- Max bpm per artist_id
    SELECT artist_id, MAX(bpm) AS max_bpm
    FROM base_tracks
    GROUP BY artist_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.max_bpm, ROUND(s.bpm - g.max_bpm, 2) AS bpm_minus_max
    FROM base_tracks s
    JOIN artist_id_stats g ON g.artist_id = s.artist_id
    WHERE s.bpm = g.max_bpm
),
with_details_artist AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.is_explicit,
        s.bpm,
        s.moods,
        s.max_bpm,
        s.bpm_minus_max,
        a.artist_name,
        a.debut_year AS artist_debut_year,
        a.country AS artist_country
    FROM vs_group s
    JOIN artists a ON a.id = s.artist_id
),
screened AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM with_details_artist s
    WHERE EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND t.duration_sec >= 164
        )
)
SELECT
    is_explicit,
    artist_country,
    artist_id,
    SUM(artist_debut_year) AS total_artist_debut_year,
    SUM(bpm) FILTER (WHERE artist_country IN ('BR', 'TH')) AS bpm_artist_country_in_br_th,
    COUNT(*) AS row_count
FROM screened
GROUP BY GROUPING SETS ((is_explicit, artist_country, artist_id), (is_explicit), ())
ORDER BY is_explicit NULLS LAST, artist_country NULLS LAST, artist_id NULLS LAST
