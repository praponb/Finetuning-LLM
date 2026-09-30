WITH base AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        COALESCE(ms_played, 0) AS ms_played_filled,
        COALESCE(ms_played, 0) AS ms_played_filled_2
    FROM plays
    WHERE played_at < TIMESTAMP '2025-03-08 10:00:00'
),
totals AS (
    -- Group-level totals
    SELECT
        track_id,
        listener_id,
        source,
        COUNT(*) AS n_rows,
        SUM(ms_played_filled_2) AS total_ms_played_filled_2
    FROM base
    GROUP BY track_id, listener_id, source
),
qualified AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM totals s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND p.played_at >= TIMESTAMP '2025-01-31 12:00:00'
              AND NOT p.is_skipped
        )
),
filtered AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM qualified s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND p.is_skipped = TRUE
              AND p.track_id IS NOT NULL
        )
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        track_id,
        listener_id,
        source,
        n_rows,
        total_ms_played_filled_2,
        MIN(total_ms_played_filled_2) OVER w_listener_id_n_rows AS listener_id_n_rows_min_total_ms_played_filled_2,
        COUNT(*) OVER (PARTITION BY track_id ORDER BY total_ms_played_filled_2 DESC NULLS FIRST, track_id, listener_id, source ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM filtered
    WINDOW w_listener_id_n_rows AS (PARTITION BY listener_id, n_rows)
),
with_parent_track AS (
    -- Attach track attributes
    SELECT
        s.track_id,
        s.listener_id,
        s.source,
        s.n_rows,
        s.running_count,
        t.title AS track_title,
        t.moods AS track_moods,
        t.bpm AS track_bpm
    FROM metrics s
    LEFT JOIN tracks t ON t.id = s.track_id
)
-- Final projection
SELECT
    track_id,
    listener_id,
    n_rows,
    running_count,
    track_title,
    track_moods,
    track_bpm,
    CONCAT(source, '#', CAST(listener_id AS STRING)) AS source_ref,
    UPPER(TRIM(track_title)) AS track_title_clean
FROM with_parent_track
ORDER BY listener_id
