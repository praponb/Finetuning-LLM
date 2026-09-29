WITH prepared_plays AS (
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
        CONCAT(source, '#', CAST(ms_played AS STRING)) AS source_ref
    FROM plays
    WHERE device NOT IN ('phone', 'speaker')
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SIZE(COLLECT_SET(device) OVER (PARTITION BY is_skipped)) AS distinct_device_in_is_skipped,
        ROUND(PERCENT_RANK() OVER (PARTITION BY source, listener_id ORDER BY ms_played), 3) AS ms_played_percent_rank,
        SUM(ms_played) OVER (PARTITION BY TO_DATE(played_at), device ORDER BY played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ms_played,
        MIN(ms_played) OVER (PARTITION BY is_skipped) AS is_skipped_min_ms_played,
        ROUND(AVG(ms_played) OVER (PARTITION BY TO_DATE(played_at), device ORDER BY played_at DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS ms_played_avg_7,
        FIRST_VALUE(played_at) IGNORE NULLS OVER (PARTITION BY TO_DATE(played_at), device ORDER BY played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_played_at
    FROM prepared_plays
),
joined_listener AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        l.interests AS listener_interests,
        l.username AS listener_username,
        l.plan AS listener_plan
    FROM with_trends s
    JOIN listeners l ON l.id = s.listener_id
)
SELECT
    listener_id,
    listener_plan,
    source,
    ROUND(STDDEV(distinct_device_in_is_skipped), 2) AS stddev_distinct_device_in_is_skipped,
    ROUND(AVG(is_skipped_min_ms_played), 2) AS avg_is_skipped_min_ms_played,
    MIN(first_known_played_at) AS first_first_known_played_at
FROM joined_listener
GROUP BY GROUPING SETS ((listener_id, listener_plan, source), (listener_id), (listener_plan))
