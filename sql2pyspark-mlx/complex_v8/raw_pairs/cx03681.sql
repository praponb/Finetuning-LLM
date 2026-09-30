WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        bpm,
        moods
    FROM tracks
    WHERE bpm > 76
),
play_stats AS (
    -- Per-track plays metrics
    SELECT
        track_id,
        MAX(ms_played) AS max_play_ms_played,
        MIN(played_at) AS first_played_at,
        COUNT(*) FILTER (WHERE device NOT IN ('car', 'phone')) AS play_device_count
    FROM plays
    GROUP BY track_id
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.max_play_ms_played,
        p.first_played_at,
        COALESCE(p.play_device_count, 0) AS play_device_count
    FROM recent s
    LEFT JOIN play_stats p ON p.track_id = s.id
),
screened AS (
    -- Keep rows whose artist matches
    SELECT s.*
    FROM with_counts s
    WHERE EXISTS (
            SELECT 1
            FROM artists a
            WHERE a.id = s.artist_id
              AND a.genre IN ('Jazz', 'Pop')
        )
),
play_stats_2 AS (
    -- Per-track plays metrics
    SELECT
        track_id,
        COUNT(*) AS play_count,
        ROUND(AVG(ms_played), 2) AS avg_play_ms_played
    FROM plays
    GROUP BY track_id
),
with_counts_2 AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_count, 0) AS play_count,
        p.avg_play_ms_played
    FROM screened s
    LEFT JOIN play_stats_2 p ON p.track_id = s.id
),
artist_id_stats AS (
    -- Avg bpm per artist_id
    SELECT artist_id, AVG(bpm) AS avg_bpm
    FROM with_counts_2
    GROUP BY artist_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_bpm, ROUND(s.bpm - g.avg_bpm, 2) AS bpm_minus_avg
    FROM with_counts_2 s
    JOIN artist_id_stats g ON g.artist_id = s.artist_id
    WHERE s.bpm > g.avg_bpm
),
with_activity AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM compared s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND t.duration_sec <= 365
              AND SIZE(t.moods) >= 1
        )
      AND s.duration_sec <= 402
)
-- Final projection
SELECT
    id,
    artist_id,
    title,
    duration_sec,
    bpm,
    moods,
    play_device_count,
    play_count,
    avg_play_ms_played,
    bpm_minus_avg,
    FLOOR(bpm_minus_avg / 50) * 50 AS bpm_minus_avg_bucket,
    LENGTH(TRIM(title)) AS title_len
FROM with_activity
ORDER BY bpm_minus_avg_bucket DESC, title
