WITH prepared_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        IF(is_explicit, 'yes', 'no') AS is_explicit_label,
        QUARTER(release_date) AS release_date_quarter,
        DATE_FORMAT(release_date, 'yyyy-MM-dd') AS release_date_label
    FROM tracks
    WHERE release_date < DATE '2025-03-29'
      AND SIZE(moods) >= 1
),
enriched_artist AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.is_explicit_label,
        s.release_date_quarter,
        s.release_date_label,
        a.artist_name
    FROM prepared_tracks s
    LEFT JOIN artists a ON a.id = s.artist_id AND UPPER(TRIM(a.artist_name)) LIKE 'S%'
),
artist_id_stats AS (
    -- Avg release_date_quarter per artist_id
    SELECT artist_id, AVG(release_date_quarter) AS avg_release_date_quarter
    FROM enriched_artist
    GROUP BY artist_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_release_date_quarter,
        ROUND(s.release_date_quarter - g.avg_release_date_quarter, 2) AS release_date_quarter_minus_avg
    FROM enriched_artist s
    JOIN artist_id_stats g ON g.artist_id = s.artist_id
    WHERE s.release_date_quarter > g.avg_release_date_quarter
),
play_stats AS (
    -- Per-track plays metrics
    SELECT track_id, MAX(ms_played) AS max_play_ms_played
    FROM plays
    GROUP BY track_id
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.max_play_ms_played
    FROM compared s
    LEFT JOIN play_stats p ON p.track_id = s.id
)
SELECT
    id,
    artist_id,
    duration_sec,
    release_date,
    is_explicit,
    bpm,
    is_explicit_label,
    artist_name,
    avg_release_date_quarter,
    release_date_quarter_minus_avg,
    max_play_ms_played,
    CASE WHEN is_explicit THEN 1 ELSE 0 END AS is_explicit_flag
FROM profiled
ORDER BY id
