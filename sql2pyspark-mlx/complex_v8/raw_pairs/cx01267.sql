WITH recent_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods
    FROM tracks
),
joined AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.moods,
        a.artist_name,
        a.country AS artist_country,
        a.debut_year AS artist_debut_year
    FROM recent_tracks s
    LEFT JOIN artists a ON a.id = s.artist_id
),
filtered AS (
    -- Keep rows whose artist matches
    SELECT s.*
    FROM joined s
    WHERE EXISTS (
            SELECT 1
            FROM artists a
            WHERE a.id = s.artist_id
              AND a.artist_name IS NOT NULL
              AND a.genre = 'Classical'
        )
),
artist_id_stats AS (
    -- Max duration_sec per artist_id
    SELECT artist_id, MAX(duration_sec) AS max_duration_sec
    FROM filtered
    GROUP BY artist_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_duration_sec,
        ROUND(s.duration_sec - g.max_duration_sec, 2) AS duration_sec_minus_max
    FROM filtered s
    JOIN artist_id_stats g ON g.artist_id = s.artist_id
    WHERE s.duration_sec = g.max_duration_sec
),
without_matches AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM compared s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND t.duration_sec > 213
              AND ARRAY_CONTAINS(t.moods, 'party')
        )
),
artist_country_stats AS (
    -- Avg duration_sec_minus_max per artist_country
    SELECT artist_country, AVG(duration_sec_minus_max) AS avg_duration_sec_minus_max
    FROM without_matches
    GROUP BY artist_country
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_duration_sec_minus_max,
        ROUND(s.duration_sec_minus_max - g.avg_duration_sec_minus_max, 2) AS duration_sec_minus_max_minus_avg
    FROM without_matches s
    JOIN artist_country_stats g ON g.artist_country = s.artist_country
    WHERE s.duration_sec_minus_max > g.avg_duration_sec_minus_max
)
-- Final projection
SELECT
    id,
    artist_id,
    duration_sec,
    release_date,
    artist_country,
    artist_debut_year,
    max_duration_sec,
    duration_sec_minus_max,
    avg_duration_sec_minus_max,
    duration_sec_minus_max_minus_avg,
    DATE_FORMAT(release_date, 'MMM yyyy') AS release_date_label,
    ABS(duration_sec - 352) AS duration_sec_distance
FROM outliers
WHERE avg_duration_sec_minus_max >= -336.0
ORDER BY duration_sec DESC NULLS FIRST
