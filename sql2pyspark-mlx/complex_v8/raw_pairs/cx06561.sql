WITH cleaned_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        moods,
        LENGTH(TRIM(title)) AS title_len,
        GREATEST(duration_sec, bpm) AS greatest_duration_sec_bpm,
        DATE_ADD(release_date, 365) AS release_date_plus_365d
    FROM tracks
    WHERE duration_sec <= 357
      AND bpm > 76
),
detail_rows_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.is_explicit,
        p.id AS play_id,
        p.source AS play_source,
        p.played_at AS play_played_at,
        p.device AS play_device,
        p.ms_played AS play_ms_played
    FROM cleaned_tracks s
    LEFT JOIN plays p ON p.track_id = s.id
),
play_stats AS (
    -- Per-track plays metrics
    SELECT track_id, ROUND(AVG(ms_played), 2) AS avg_play_ms_played
    FROM plays
    GROUP BY track_id
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.avg_play_ms_played
    FROM detail_rows_plays s
    LEFT JOIN play_stats p ON p.track_id = s.id
),
play_device_stats AS (
    -- Max avg_play_ms_played per play_device
    SELECT play_device, MAX(avg_play_ms_played) AS max_avg_play_ms_played
    FROM profiled
    GROUP BY play_device
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_avg_play_ms_played,
        ROUND(s.avg_play_ms_played - g.max_avg_play_ms_played, 2) AS avg_play_ms_played_minus_max
    FROM profiled s
    JOIN play_device_stats g ON g.play_device = s.play_device
    WHERE s.avg_play_ms_played = g.max_avg_play_ms_played
),
screened AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM vs_group s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND LOWER(t.title) LIKE '%on%'
        )
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        artist_id,
        is_explicit,
        play_id,
        play_source,
        play_ms_played,
        avg_play_ms_played,
        max_avg_play_ms_played,
        avg_play_ms_played_minus_max,
        ROUND(avg_play_ms_played / NULLIF(play_ms_played, 0), 3) AS avg_play_ms_played_per_play_ms_played,
        COALESCE(avg_play_ms_played, 0.0) AS avg_play_ms_played_filled,
        play_played_at + INTERVAL 15 MINUTES AS play_played_at_plus_15m
    FROM screened
    WHERE is_explicit = TRUE
)
SELECT
    id,
    is_explicit,
    play_id,
    play_source,
    play_ms_played,
    max_avg_play_ms_played,
    avg_play_ms_played_minus_max,
    avg_play_ms_played_per_play_ms_played,
    avg_play_ms_played_filled,
    play_played_at_plus_15m
FROM scored
ORDER BY play_ms_played DESC, play_source
