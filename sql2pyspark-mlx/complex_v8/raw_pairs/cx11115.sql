WITH recent_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        CASE WHEN device IN ('desktop', 'car') THEN 'group_a' WHEN device = 'phone' THEN 'group_b' ELSE 'other' END AS device_group,
        CASE WHEN ms_played >= 318978 THEN 'large' WHEN ms_played >= 73177 THEN 'medium' ELSE 'small' END AS ms_played_tier
    FROM plays
    WHERE listener_id IS NOT NULL
      AND played_at >= TIMESTAMP '2025-01-19 11:00:00'
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        device_group,
        listener_id,
        ARRAY_SORT(COLLECT_SET(device)) AS device_list,
        COUNT(DISTINCT track_id) AS distinct_track_ids
    FROM recent_plays
    GROUP BY 1, 2
    HAVING AVG(ms_played) > 98052
),
exploded AS (
    -- One row per device_list_item
    SELECT
        s.device_group,
        s.listener_id,
        s.distinct_track_ids,
        t.device_list_item
    FROM per_group s
    LATERAL VIEW EXPLODE(s.device_list) t AS device_list_item
)
SELECT
    device_group,
    listener_id,
    distinct_track_ids,
    device_list_item
FROM exploded
ORDER BY device_group, listener_id DESC NULLS FIRST, device_list_item
LIMIT 10
