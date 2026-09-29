WITH recent AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        SUBSTRING(TRIM(title), 1, 3) AS title_prefix
    FROM tracks
    WHERE duration_sec > 183
),
artist_id_stats AS (
    -- Max bpm per artist_id
    SELECT artist_id, MAX(bpm) AS max_bpm
    FROM recent
    GROUP BY artist_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.max_bpm, ROUND(s.bpm - g.max_bpm, 2) AS bpm_minus_max
    FROM recent s
    JOIN artist_id_stats g ON g.artist_id = s.artist_id
    WHERE s.bpm = g.max_bpm
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        DATEDIFF(DATE '2025-12-31', release_date) AS days_since_release_date,
        LOWER(REGEXP_REPLACE(TRIM(title_prefix), ' ', '_')) AS title_prefix_slug,
        CONCAT_WS(': ', title_prefix, title) AS title_prefix_title_label
    FROM outliers
    WHERE duration_sec < 402
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MAX(p.played_at) FROM plays p WHERE p.track_id = s.id) AS last_played_at,
        (SELECT SUM(p.ms_played) FROM plays p WHERE p.track_id = s.id) AS total_play_ms_played,
        (SELECT COUNT(*) FILTER (WHERE p.is_skipped = TRUE) FROM plays p WHERE p.track_id = s.id) AS play_is_skipped_count
    FROM annotated s
),
line_level AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.release_date,
        s.bpm,
        s.max_bpm,
        s.bpm_minus_max,
        s.days_since_release_date,
        s.total_play_ms_played,
        s.play_is_skipped_count,
        p.id AS play_id,
        p.is_skipped AS play_is_skipped,
        p.played_at AS play_played_at,
        p.ms_played AS play_ms_played
    FROM profiled s
    LEFT JOIN plays p ON p.track_id = s.id
)
-- Final projection
SELECT
    id,
    artist_id,
    release_date,
    bpm,
    max_bpm,
    bpm_minus_max,
    total_play_ms_played,
    play_is_skipped_count,
    play_id,
    play_played_at,
    play_ms_played,
    LEAST(bpm_minus_max, days_since_release_date) AS least_bpm_minus_max_days_since_release_date,
    ROUND(max_bpm / NULLIF(total_play_ms_played, 0), 3) AS max_bpm_per_total_play_ms_played
FROM line_level
ORDER BY max_bpm DESC, bpm_minus_max DESC
