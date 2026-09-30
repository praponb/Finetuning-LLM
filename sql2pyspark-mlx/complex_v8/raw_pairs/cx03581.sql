WITH valid_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        DATE_TRUNC('HOUR', played_at) AS played_at_hour,
        CONCAT_WS('-', device, source) AS device_source_label,
        COALESCE(source, 'UNKNOWN') AS source_or_unknown
    FROM plays
    WHERE is_skipped = TRUE
),
qualified AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM valid_plays s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND p.ms_played >= 92277
              AND p.track_id IS NOT NULL
        )
      AND s.played_at_hour < TIMESTAMP '2025-02-05 09:00:00'
),
play_stats AS (
    -- Per-listener plays metrics
    SELECT listener_id, MAX(played_at) AS last_played_at, COUNT(*) AS play_count
    FROM plays
    GROUP BY listener_id
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.last_played_at,
        COALESCE(p.play_count, 0) AS play_count
    FROM qualified s
    LEFT JOIN play_stats p ON p.listener_id = s.listener_id
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.device,
        s.is_skipped,
        s.source,
        s.played_at_hour,
        s.device_source_label,
        s.source_or_unknown,
        t.release_date AS track_release_date,
        t.artist_id AS track_artist_id,
        t.moods AS track_moods
    FROM summarized_rows s
    LEFT JOIN tracks t ON t.id = s.track_id AND t.release_date >= DATE '2024-10-23'
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        track_id,
        device,
        is_skipped,
        source,
        device_source_label,
        source_or_unknown,
        track_artist_id,
        track_moods,
        SIZE(COLLECT_SET(source_or_unknown) OVER (PARTITION BY listener_id)) AS distinct_source_or_unknown_in_listener_id,
        ROW_NUMBER() OVER w_track_artist_id_by_played_at AS seq_no,
        ROUND(AVG(ms_played) OVER (PARTITION BY listener_id), 2) AS listener_id_avg_ms_played
    FROM joined
    WINDOW w_track_artist_id_by_played_at AS (PARTITION BY track_artist_id ORDER BY played_at_hour DESC, id)
)
SELECT DISTINCT track_artist_id
FROM sequenced
ORDER BY track_artist_id
