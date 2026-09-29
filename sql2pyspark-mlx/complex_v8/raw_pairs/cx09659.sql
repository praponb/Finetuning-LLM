WITH prepared_plays AS (
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
        DATE_FORMAT(played_at, 'yyyy-MM') AS played_at_label
    FROM plays
    WHERE played_at >= TIMESTAMP '2025-01-13 12:00:00'
      AND listener_id IS NOT NULL
),
latest AS (
    -- Keep the latest row per source, track_id
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        played_at_label
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY source, track_id ORDER BY ms_played DESC, id DESC) AS rn
        FROM prepared_plays
    ) ranked
    WHERE rn = 1
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(ms_played - AVG(ms_played) OVER w_source, 2) AS ms_played_vs_source_avg,
        ROW_NUMBER() OVER (PARTITION BY listener_id ORDER BY played_at, id) AS rn,
        ROUND(PERCENT_RANK() OVER w_listener_id_by_played_at_tie, 3) AS played_at_percent_rank,
        COUNT(*) OVER (PARTITION BY source) AS rows_in_source,
        LEAD(ms_played) OVER w_listener_id_by_played_at AS next_ms_played
    FROM latest
    WINDOW w_source AS (PARTITION BY source), w_listener_id_by_played_at_tie AS (PARTITION BY listener_id ORDER BY played_at), w_listener_id_by_played_at AS (PARTITION BY listener_id ORDER BY played_at, id)
)
-- Subtotals with ROLLUP
SELECT
    rn,
    device,
    COUNT(*) AS row_count,
    ROUND(AVG(played_at_percent_rank), 2) AS avg_played_at_percent_rank,
    SUM(CASE WHEN device = 'speaker' THEN 1 ELSE 0 END) AS n_device_speaker
FROM windowed
GROUP BY ROLLUP(rn, device)
