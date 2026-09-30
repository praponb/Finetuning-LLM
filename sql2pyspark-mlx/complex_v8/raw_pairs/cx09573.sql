WITH valid AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        is_skipped,
        source,
        LOWER(REGEXP_REPLACE(TRIM(device), ' ', '_')) AS device_slug,
        COALESCE(device, 'UNKNOWN') AS device_or_unknown
    FROM plays
    WHERE source IS NOT NULL
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(played_at, 2) OVER (PARTITION BY device_or_unknown ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY device_or_unknown ORDER BY played_at), 3) AS played_at_percent_rank,
        FIRST_VALUE(played_at) IGNORE NULLS OVER (PARTITION BY device_or_unknown ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_played_at,
        LAG(played_at) OVER (PARTITION BY device_or_unknown ORDER BY played_at, id) AS prev_played_at,
        LAST_VALUE(played_at) IGNORE NULLS OVER (ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_played_at
    FROM valid
    WINDOW w_device_or_unknown_by_played_4 AS (PARTITION BY device_or_unknown ORDER BY played_at, id)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT listener_id, first_known_played_at, source, played_at_percent_rank
    FROM windowed
) src
PIVOT (
    SUM(played_at_percent_rank)
    FOR source IN ('playlist', 'radio', 'search')
)
