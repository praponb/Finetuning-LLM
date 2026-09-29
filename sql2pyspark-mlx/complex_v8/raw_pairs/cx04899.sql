WITH prepared_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        moods
    FROM tracks
    WHERE ARRAY_CONTAINS(moods, 'focus')
      AND bpm BETWEEN 76 AND 165
),
line_level AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.is_explicit,
        p.id AS play_id,
        p.played_at AS play_played_at,
        p.is_skipped AS play_is_skipped,
        p.listener_id AS play_listener_id
    FROM prepared_tracks s
    JOIN plays p ON p.track_id = s.id
),
screened AS (
    -- Keep rows whose listener matches
    SELECT s.*
    FROM line_level s
    WHERE NOT EXISTS (
            SELECT 1
            FROM listeners l
            WHERE l.id = s.play_listener_id
              AND l.birth_year BETWEEN 1966 AND 1995
        )
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        play_id,
        play_played_at,
        TO_DATE(play_played_at) AS play_played_at_date
    FROM screened
    WHERE play_id IS NOT NULL
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MAX(p.played_at) FROM plays p WHERE p.track_id = s.id) AS last_played_at,
        (SELECT MIN(p.ms_played) FROM plays p WHERE p.track_id = s.id) AS min_play_ms_played,
        (SELECT MIN(p.played_at) FROM plays p WHERE p.track_id = s.id) AS first_played_at
    FROM tagged s
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', play_played_at) AS play_played_at_month,
        DATE_FORMAT(play_played_at_date, 'yyyy-MM') AS play_played_at_date_ym,
        COUNT(*) AS row_count,
        COUNT(DISTINCT play_id) AS distinct_play_ids
    FROM with_stats
    WHERE YEAR(play_played_at_date) = 2024
    GROUP BY 1, 2
)
-- Final projection
SELECT play_played_at_month, row_count, distinct_play_ids
FROM rollup_base
ORDER BY row_count
