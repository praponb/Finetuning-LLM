WITH scoped AS (
    -- Scope the artists rows
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        is_verified,
        SUBSTRING(TRIM(genre), 1, 2) AS genre_prefix,
        CASE WHEN debut_year >= 2016 THEN 'HIGH' WHEN debut_year >= 1996 THEN 'MEDIUM' ELSE 'LOW' END AS debut_year_tier,
        LOWER(REGEXP_REPLACE(TRIM(genre), ' ', '_')) AS genre_slug
    FROM artists
    WHERE debut_year >= 1997
),
line_level AS (
    -- One row per track
    SELECT
        s.id,
        s.artist_name,
        s.country,
        s.debut_year,
        t.id AS track_id,
        t.release_date AS track_release_date,
        t.bpm AS track_bpm
    FROM scoped s
    LEFT JOIN tracks t ON t.artist_id = s.id
),
with_parent AS (
    -- Join tracks
    SELECT
        s.*,
        t.release_date AS track_release_date_2,
        t.duration_sec AS track_duration_sec,
        t.title AS track_title
    FROM line_level s
    JOIN tracks t ON t.id = s.track_id
    WHERE t.title IS NOT NULL
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT SUM(p.ms_played) FROM plays p WHERE p.track_id = s.track_id) AS total_play_ms_played,
        (SELECT MAX(p.played_at) FROM plays p WHERE p.track_id = s.track_id) AS last_played_at,
        (SELECT COUNT(*) FROM plays p WHERE p.track_id = s.track_id) AS play_count
    FROM with_parent s
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT track_release_date, country, play_count
    FROM profiled
) src
PIVOT (
    SUM(play_count)
    FOR country IN ('KR', 'JP')
)
ORDER BY track_release_date
