WITH base_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        COALESCE(ms_played, 0) AS ms_played_filled,
        DATEDIFF(DATE '2026-01-01', played_at) AS days_since_played_at
    FROM plays
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_track_id_device_by_played_at, 3) AS played_at_percent_rank,
        LAST_VALUE(days_since_played_at) IGNORE NULLS OVER (PARTITION BY listener_id, track_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_days_since_played_at,
        MAX(ms_played) OVER (PARTITION BY device, listener_id) AS device_listener_id_max_ms_played,
        DENSE_RANK() OVER (PARTITION BY track_id, device ORDER BY played_at DESC, id) AS played_at_dense_rank
    FROM base_plays
    WINDOW w_track_id_device_by_played_at AS (PARTITION BY track_id, device ORDER BY played_at DESC)
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        ms_played_filled,
        days_since_played_at,
        last_known_days_since_played_at,
        device_listener_id_max_ms_played,
        played_at_dense_rank,
        MIN(played_at_dense_rank) OVER (ORDER BY days_since_played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_played_at_dense_rank_so_far,
        FIRST_VALUE(played_at_percent_rank) IGNORE NULLS OVER (PARTITION BY is_skipped, device ORDER BY device_listener_id_max_ms_played ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_played_at_percent_rank
    FROM with_windows
),
qualified AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM with_trends s
    WHERE s.track_id IN (
            SELECT p.track_id
            FROM plays p
            WHERE p.device IN ('phone', 'desktop')
              AND p.listener_id IS NOT NULL
        )
      AND s.last_known_days_since_played_at BETWEEN 210 AND 595
),
leaders AS (
    -- Top 2 per group
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        ms_played_filled,
        days_since_played_at,
        last_known_days_since_played_at,
        device_listener_id_max_ms_played,
        min_played_at_dense_rank_so_far,
        first_known_played_at_percent_rank
    FROM qualified
    WHERE played_at_dense_rank <= 2
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT listener_id, track_id, device, min_played_at_dense_rank_so_far
    FROM leaders
) src
PIVOT (
    MIN(min_played_at_dense_rank_so_far)
    FOR device IN ('phone', 'speaker', 'car')
)
ORDER BY listener_id, track_id
