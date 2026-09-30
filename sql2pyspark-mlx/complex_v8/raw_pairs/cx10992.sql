WITH base AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source,
        YEAR(played_at) AS played_at_year,
        LENGTH(TRIM(source)) AS source_len
    FROM plays
    WHERE NOT is_skipped
      AND MONTH(played_at) IN (1, 2)
),
with_parent_track AS (
    -- Attach track attributes
    SELECT s.*, t.release_date AS track_release_date
    FROM base s
    LEFT JOIN tracks t ON t.id = s.track_id
),
filtered AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM with_parent_track s
    WHERE s.track_id IN (
            SELECT p.track_id
            FROM plays p
            WHERE p.ms_played > 130888
        )
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        played_at_year,
        source_len,
        LEAD(source_len, 2) OVER (ORDER BY played_at DESC, id) AS next2_source_len,
        NTILE(3) OVER (ORDER BY played_at DESC, id) AS played_at_tercile,
        source_len - LAG(source_len) OVER (ORDER BY played_at DESC, id) AS source_len_change,
        ROUND(CUME_DIST() OVER (ORDER BY played_at DESC), 3) AS played_at_cume_dist
    FROM filtered
)
-- Subtotals with ROLLUP
SELECT
    played_at_tercile,
    COALESCE(device, 'ALL') AS device,
    listener_id,
    COUNT(*) AS row_count
FROM windowed
GROUP BY ROLLUP(played_at_tercile, device, listener_id)
