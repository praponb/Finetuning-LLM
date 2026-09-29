WITH base AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        release_date,
        moods
    FROM tracks
    WHERE (ARRAY_CONTAINS(moods, 'sad') OR bpm >= 76)
),
with_activity AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND t.duration_sec > 165
        )
      AND ARRAY_CONTAINS(s.moods, 'chill')
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        DATEDIFF(DATE '2026-01-01', release_date) AS days_since_release_date,
        SUBSTRING(TRIM(title), 1, 3) AS title_prefix
    FROM with_activity
),
with_counts AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM tracks t WHERE t.artist_id = s.artist_id) AS track_count
    FROM tagged s
),
play_stats AS (
    -- Per-track plays metrics
    SELECT
        track_id,
        MAX(played_at) AS last_played_at,
        MIN(ms_played) AS min_play_ms_played,
        COUNT(*) AS play_count
    FROM plays
    GROUP BY track_id
),
with_counts_2 AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.last_played_at,
        p.min_play_ms_played,
        COALESCE(p.play_count, 0) AS play_count
    FROM with_counts s
    LEFT JOIN play_stats p ON p.track_id = s.id
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        LENGTH(TRIM(title_prefix)) AS title_prefix_len,
        ARRAY_CONTAINS(moods, 'sad') AS has_sad
    FROM with_counts_2
),
artist_id_stats AS (
    -- Avg days_since_release_date per artist_id
    SELECT artist_id, AVG(days_since_release_date) AS avg_days_since_release_date
    FROM enriched
    GROUP BY artist_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_days_since_release_date,
        ROUND(s.days_since_release_date - g.avg_days_since_release_date, 2) AS days_since_release_date_minus_avg
    FROM enriched s
    JOIN artist_id_stats g ON g.artist_id = s.artist_id
    WHERE s.days_since_release_date > g.avg_days_since_release_date
)
-- Final projection
SELECT
    id,
    artist_id,
    moods,
    days_since_release_date,
    title_prefix,
    track_count,
    min_play_ms_played,
    has_sad,
    avg_days_since_release_date,
    days_since_release_date_minus_avg,
    DATE_ADD(release_date, 365) AS release_date_plus_365d,
    DATE_FORMAT(release_date, 'yyyy-MM-dd') AS release_date_label
FROM compared
ORDER BY days_since_release_date_minus_avg, id
LIMIT 5
