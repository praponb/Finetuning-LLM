WITH base AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        CASE WHEN ms_played >= 308104 THEN 'large' WHEN ms_played >= 107912 THEN 'medium' ELSE 'small' END AS ms_played_tier
    FROM plays
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(listener_id) OVER w_device) AS distinct_listener_id_in_device,
        LAST_VALUE(ms_played) OVER (ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_ms_played,
        COUNT(*) OVER (PARTITION BY device) AS rows_in_device,
        SUM(ms_played) OVER (ORDER BY played_at, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS ms_played_sum_4
    FROM base
    WINDOW w_device AS (PARTITION BY device)
),
with_activity AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM ranked s
    WHERE s.track_id IN (
            SELECT p.track_id
            FROM plays p
            WHERE p.listener_id >= 7
        )
      AND s.is_skipped = TRUE
)
SELECT *
FROM (
    SELECT listener_id, source, distinct_listener_id_in_device
    FROM with_activity
) src
PIVOT (
    SUM(distinct_listener_id_in_device) AS amount, COUNT(distinct_listener_id_in_device) AS n
    FOR source IN ('playlist', 'radio', 'album', 'search')
)
ORDER BY listener_id
