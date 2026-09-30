WITH prepared_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        moods,
        SUBSTRING(TRIM(title), 1, 3) AS title_prefix,
        FLOOR(duration_sec / 50) * 50 AS duration_sec_bucket
    FROM tracks
    WHERE bpm > 94
      AND SIZE(moods) >= 1
),
joined_artist AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.moods,
        s.title_prefix,
        a.is_verified AS artist_is_verified,
        a.genre AS artist_genre
    FROM prepared_tracks s
    LEFT JOIN artists a ON a.id = s.artist_id
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM joined_artist
    WHERE duration_sec > (SELECT AVG(duration_sec) FROM joined_artist)
),
artist_id_stats AS (
    -- Max duration_sec per artist_id
    SELECT artist_id, MAX(duration_sec) AS max_duration_sec
    FROM with_activity
    GROUP BY artist_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_duration_sec,
        ROUND(s.duration_sec - g.max_duration_sec, 2) AS duration_sec_minus_max
    FROM with_activity s
    JOIN artist_id_stats g ON g.artist_id = s.artist_id
    WHERE s.duration_sec = g.max_duration_sec
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        DATE_TRUNC('WEEK', release_date) AS release_date_week,
        LENGTH(TRIM(artist_genre)) AS artist_genre_len,
        CONCAT(title_prefix, '#', CAST(id AS STRING)) AS title_prefix_ref
    FROM above_average
)
-- Final projection
SELECT
    id,
    artist_id,
    release_date,
    moods,
    title_prefix,
    artist_genre,
    max_duration_sec,
    release_date_week,
    artist_genre_len,
    COALESCE(artist_genre, 'UNKNOWN') AS artist_genre_or_unknown
FROM tagged
ORDER BY artist_genre, max_duration_sec DESC NULLS FIRST
