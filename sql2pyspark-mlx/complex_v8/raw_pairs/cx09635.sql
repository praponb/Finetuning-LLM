WITH scoped AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        moods,
        YEAR(release_date) AS release_date_year,
        DATE_FORMAT(release_date, 'yyyy-MM-dd') AS release_date_label
    FROM tracks
    WHERE YEAR(release_date) = 2025
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        artist_id,
        release_date,
        is_explicit,
        release_date_year,
        release_date_label,
        ROUND(duration_sec - AVG(duration_sec) OVER (PARTITION BY release_date_year), 2) AS duration_sec_vs_release_date_year_avg,
        ROW_NUMBER() OVER w_artist_id_by_duration_sec AS rn
    FROM scoped
    WINDOW w_artist_id_by_duration_sec AS (PARTITION BY artist_id ORDER BY duration_sec, id)
),
enriched_artist AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.release_date,
        s.release_date_year,
        s.duration_sec_vs_release_date_year_avg,
        s.rn,
        a.artist_name,
        a.genre AS artist_genre
    FROM metrics s
    LEFT JOIN artists a ON a.id = s.artist_id AND LOWER(a.artist_name) LIKE '%ig%'
),
artist_genre_stats AS (
    -- Max rn per artist_genre
    SELECT artist_genre, MAX(rn) AS max_rn
    FROM enriched_artist
    GROUP BY artist_genre
),
compared AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.max_rn, ROUND(s.rn - g.max_rn, 2) AS rn_minus_max
    FROM enriched_artist s
    JOIN artist_genre_stats g ON g.artist_genre = s.artist_genre
    WHERE s.rn = g.max_rn
),
detail_rows AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.release_date_year,
        s.duration_sec_vs_release_date_year_avg,
        s.rn,
        s.artist_name,
        s.max_rn,
        s.rn_minus_max,
        p.id AS play_id,
        p.listener_id AS play_listener_id,
        p.ms_played AS play_ms_played,
        p.device AS play_device,
        p.source AS play_source
    FROM compared s
    LEFT JOIN plays p ON p.track_id = s.id
)
-- Subtotals with GROUPING SETS
SELECT
    release_date_year,
    artist_id,
    COALESCE(play_device, 'ALL') AS play_device,
    GROUPING_ID() AS gid,
    COUNT(DISTINCT play_source) AS distinct_play_sources,
    ARRAY_SORT(COLLECT_SET(play_device)) AS play_device_list
FROM detail_rows
GROUP BY GROUPING SETS ((release_date_year, artist_id, play_device), (release_date_year), ())
ORDER BY release_date_year NULLS LAST, artist_id NULLS LAST, play_device NULLS LAST
