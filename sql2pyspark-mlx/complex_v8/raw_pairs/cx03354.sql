WITH valid AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped
    FROM plays
    WHERE (is_skipped = TRUE OR listener_id IS NOT NULL)
),
qualified AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM valid s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND p.track_id IS NOT NULL
              AND p.ms_played < 261579
        )
      AND TO_DATE(s.played_at) < DATE '2025-03-08'
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(ms_played, 3, 0) OVER (ORDER BY played_at DESC, id) AS next3_ms_played,
        COUNT(*) OVER (PARTITION BY device) AS rows_in_device
    FROM qualified
)
-- Subtotals with GROUPING SETS
SELECT
    is_skipped,
    listener_id,
    ROUND(SUM(rows_in_device) / NULLIF(SUM(next3_ms_played), 0), 3) AS rows_in_device_to_next3_ms_played_ratio,
    MAX(played_at) AS last_played_at,
    COUNT(*) FILTER (WHERE is_skipped = FALSE) AS n_is_skipped_false
FROM windowed
GROUP BY GROUPING SETS ((is_skipped, listener_id), (is_skipped), (listener_id))
ORDER BY is_skipped NULLS LAST, listener_id NULLS LAST
