WITH base AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped
    FROM plays
    WHERE device = 'desktop'
),
latest AS (
    -- Keep the earliest row per device
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY device ORDER BY played_at, id) AS row_num
        FROM base
    ) ranked
    WHERE row_num = 1
),
with_details AS (
    -- Attach listener attributes
    SELECT s.*, l.country AS listener_country, l.plan AS listener_plan
    FROM latest s
    LEFT JOIN listeners l ON l.id = s.listener_id
),
qualified AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM with_details s
    WHERE s.listener_id IN (
            SELECT p.listener_id
            FROM plays p
            WHERE p.device <> 'car'
              AND p.is_skipped = FALSE
        )
)
-- Subtotals with GROUPING SETS
SELECT
    is_skipped,
    listener_id,
    COUNT(DISTINCT listener_id) AS distinct_listener_ids,
    COUNT(DISTINCT track_id) AS distinct_track_ids,
    COUNT(*) FILTER (WHERE device = 'tv') AS n_device_tv
FROM qualified
GROUP BY GROUPING SETS ((is_skipped, listener_id), (is_skipped), (listener_id), ())
