WITH valid AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped
    FROM plays
    WHERE listener_id >= 15
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        is_skipped,
        ARRAY_SORT(COLLECT_SET(device)) AS device_list,
        COUNT(DISTINCT listener_id) AS distinct_listener_ids
    FROM valid
    WHERE listener_id IS NOT NULL
    GROUP BY 1
)
SELECT is_skipped, distinct_listener_ids
FROM rollup_base
WHERE distinct_listener_ids > 2
ORDER BY distinct_listener_ids
