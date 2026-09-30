WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        CONCAT_WS('|', source, device) AS source_device_label
    FROM plays
    WHERE listener_id IS NOT NULL
),
grouped AS (
    -- Group-level totals
    SELECT
        is_skipped,
        track_id,
        MIN(played_at) AS first_played_at,
        SUM(ms_played) AS total_ms_played,
        MAX(ms_played) AS max_ms_played
    FROM valid
    GROUP BY is_skipped, track_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        is_skipped,
        track_id,
        first_played_at,
        total_ms_played,
        NTILE(10) OVER w_all_by_max_ms_played AS max_ms_played_decile,
        COUNT(*) OVER (PARTITION BY track_id, is_skipped) AS rows_in_track_id_is_skipped,
        MAX(total_ms_played) OVER (ORDER BY max_ms_played ASC, is_skipped, track_id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS total_ms_played_max_4
    FROM grouped
    WINDOW w_all_by_max_ms_played AS (ORDER BY max_ms_played ASC, is_skipped, track_id)
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.is_skipped,
        s.track_id,
        s.first_played_at,
        s.max_ms_played_decile,
        s.rows_in_track_id_is_skipped,
        s.total_ms_played_max_4,
        t.moods AS track_moods,
        t.title AS track_title,
        t.duration_sec AS track_duration_sec,
        a.debut_year AS artist_debut_year
    FROM windowed s
    LEFT JOIN tracks t ON t.id = s.track_id
    LEFT JOIN artists a ON a.id = t.artist_id
)
-- Final projection
SELECT
    is_skipped,
    first_played_at,
    max_ms_played_decile,
    rows_in_track_id_is_skipped,
    total_ms_played_max_4,
    track_title,
    track_duration_sec,
    artist_debut_year
FROM with_parent
WHERE is_skipped = FALSE
ORDER BY track_duration_sec DESC, track_title
