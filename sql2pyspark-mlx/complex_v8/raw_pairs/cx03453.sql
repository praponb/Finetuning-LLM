WITH scoped_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        moods,
        LENGTH(TRIM(title)) AS title_len,
        LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug,
        bpm - duration_sec AS bpm_minus_duration_sec
    FROM tracks
    WHERE LOWER(title) LIKE '%ne%'
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.release_date,
        s.is_explicit,
        s.title_len,
        s.title_slug,
        s.bpm_minus_duration_sec,
        a.is_verified AS artist_is_verified,
        a.artist_name
    FROM scoped_tracks s
    LEFT JOIN artists a ON a.id = s.artist_id
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        artist_id,
        release_date,
        title_slug,
        bpm_minus_duration_sec,
        artist_is_verified,
        artist_name,
        COALESCE(title_len, 0) AS title_len_filled
    FROM enriched
),
without_matches AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM tagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
              AND p.source IS NOT NULL
              AND p.listener_id <= 2
        )
),
artist_id_stats AS (
    -- Avg title_len_filled per artist_id
    SELECT artist_id, AVG(title_len_filled) AS avg_title_len_filled
    FROM without_matches
    GROUP BY artist_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_title_len_filled,
        ROUND(s.title_len_filled - g.avg_title_len_filled, 2) AS title_len_filled_minus_avg
    FROM without_matches s
    JOIN artist_id_stats g ON g.artist_id = s.artist_id
    WHERE s.title_len_filled > g.avg_title_len_filled
),
detail_rows AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.bpm_minus_duration_sec,
        s.title_len_filled,
        s.title_len_filled_minus_avg,
        p.id AS play_id,
        p.played_at AS play_played_at,
        p.listener_id AS play_listener_id
    FROM above_average s
    JOIN plays p ON p.track_id = s.id AND p.listener_id IS NOT NULL
),
combined_details_listener AS (
    -- Join listeners
    SELECT
        s.id,
        s.artist_id,
        s.bpm_minus_duration_sec,
        s.title_len_filled,
        s.title_len_filled_minus_avg,
        s.play_id,
        s.play_played_at,
        s.play_listener_id,
        l.country AS listener_country,
        l.birth_year AS listener_birth_year
    FROM detail_rows s
    JOIN listeners l ON l.id = s.play_listener_id
)
-- Final projection
SELECT
    id,
    artist_id,
    bpm_minus_duration_sec,
    title_len_filled,
    title_len_filled_minus_avg,
    play_id,
    play_played_at,
    listener_country,
    listener_birth_year,
    ABS(bpm_minus_duration_sec - 411) AS bpm_minus_duration_sec_distance,
    LENGTH(TRIM(listener_country)) AS listener_country_len
FROM combined_details_listener
WHERE play_listener_id <= 4
ORDER BY artist_id DESC
