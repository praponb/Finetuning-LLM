WITH prepared AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        IF(is_skipped, 'yes', 'no') AS is_skipped_label
    FROM plays
    WHERE played_at < TIMESTAMP '2025-02-12 09:00:00'
      AND track_id >= 5
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM prepared
    WHERE ms_played > (SELECT AVG(ms_played) FROM prepared)
),
labeled AS (
    -- Add calculated columns
    SELECT *, DATEDIFF(DATE '2025-06-30', played_at) AS days_since_played_at
    FROM qualified
),
without_matches AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM labeled s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
        )
)
-- Subtotals with CUBE
SELECT
    track_id,
    COALESCE(is_skipped_label, 'ALL') AS is_skipped_label,
    SUM(CASE WHEN source = 'search' THEN 1 ELSE 0 END) AS n_source_search
FROM without_matches
GROUP BY CUBE(track_id, is_skipped_label)
ORDER BY track_id NULLS LAST, is_skipped_label NULLS LAST
