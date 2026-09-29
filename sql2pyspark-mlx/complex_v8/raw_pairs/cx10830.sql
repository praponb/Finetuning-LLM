WITH valid_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        release_date,
        moods
    FROM tracks
    WHERE UPPER(TRIM(title)) LIKE 'G%'
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MAX(p.played_at) FROM plays p WHERE p.track_id = s.id) AS last_played_at,
        (SELECT COUNT(*) FROM plays p WHERE p.track_id = s.id) AS play_count,
        (SELECT SUM(p.ms_played) FROM plays p WHERE p.track_id = s.id) AS total_play_ms_played
    FROM valid_tracks s
),
enriched AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.release_date,
        s.moods,
        s.last_played_at,
        s.play_count,
        s.total_play_ms_played,
        a.debut_year AS artist_debut_year,
        a.country AS artist_country
    FROM summarized_rows s
    LEFT JOIN artists a ON a.id = s.artist_id
),
screened AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
              AND p.device IS NOT NULL
              AND p.played_at < TIMESTAMP '2025-02-24 10:00:00'
        )
),
artist_id_stats AS (
    -- Avg total_play_ms_played per artist_id
    SELECT artist_id, AVG(total_play_ms_played) AS avg_total_play_ms_played
    FROM screened
    GROUP BY artist_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_total_play_ms_played,
        ROUND(s.total_play_ms_played - g.avg_total_play_ms_played, 2) AS total_play_ms_played_minus_avg
    FROM screened s
    JOIN artist_id_stats g ON g.artist_id = s.artist_id
    WHERE s.total_play_ms_played > g.avg_total_play_ms_played
),
enriched_2 AS (
    -- Attach artist attributes
    SELECT
        s.*,
        a.debut_year AS artist_debut_year_2,
        a.genre AS artist_genre,
        a.is_verified AS artist_is_verified
    FROM outliers s
    JOIN artists a ON a.id = s.artist_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT artist_id, artist_genre, artist_country, avg_total_play_ms_played
    FROM enriched_2
) src
PIVOT (
    MIN(avg_total_play_ms_played) AS min_v, SUM(avg_total_play_ms_played) AS sum_v
    FOR artist_country IN ('JP', 'BR', 'KR')
)
