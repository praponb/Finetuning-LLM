WITH base AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source
    FROM plays
    WHERE is_skipped = TRUE
      AND ms_played > 160264
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        ABS(ms_played - 160264) AS ms_played_distance,
        played_at + INTERVAL 2 HOURS AS played_at_plus_2h,
        DATE_TRUNC('MONTH', played_at) AS played_at_month
    FROM base
),
latest AS (
    -- Keep the latest row per device, source
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        ms_played_distance,
        played_at_plus_2h,
        played_at_month
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY device, source ORDER BY played_at_plus_2h DESC, id DESC) AS rn
        FROM scored
    ) ranked
    WHERE rn = 1
),
qualified AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND p.device = 'car'
              AND p.is_skipped = FALSE
        )
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN source IN ('search', 'album') THEN 'group_a' WHEN source = 'radio' THEN 'group_b' ELSE 'other' END AS source_group,
        ms_played + ms_played_distance AS ms_played_plus_ms_played_distance
    FROM qualified
    WHERE TO_DATE(played_at_plus_2h) < DATE '2025-02-13'
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT played_at_plus_2h, device, source_group, ms_played
    FROM enriched
) src
PIVOT (
    MAX(ms_played)
    FOR source_group IN ('group_a', 'group_b')
)
ORDER BY played_at_plus_2h, device
