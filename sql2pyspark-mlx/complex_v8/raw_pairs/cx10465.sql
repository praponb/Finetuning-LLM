WITH prepared_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        DATE_TRUNC('MONTH', played_at) AS played_at_month,
        CASE WHEN source IN ('search', 'radio') THEN 'group_a' WHEN source = 'playlist' THEN 'group_b' ELSE 'other' END AS source_group,
        COALESCE(device, 'UNKNOWN') AS device_or_unknown
    FROM plays
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        listener_id,
        is_skipped,
        played_at_month,
        source_group,
        device_or_unknown,
        NTILE(10) OVER w_all_by_played_at_month AS played_at_month_decile
    FROM prepared_plays
    WINDOW w_all_by_played_at_month AS (ORDER BY played_at_month, id)
),
ranked_2 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(played_at_month_decile) OVER (PARTITION BY device_or_unknown), 2) AS device_or_unknown_avg_played_at_month_decile,
        ROUND(CUME_DIST() OVER (PARTITION BY device_or_unknown, played_at_month_decile ORDER BY played_at_month_decile DESC), 3) AS played_at_month_decile_cume_dist,
        LAST_VALUE(played_at_month) IGNORE NULLS OVER (PARTITION BY device_or_unknown, played_at_month_decile ORDER BY played_at_month_decile DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_played_at_month
    FROM ranked
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        is_skipped,
        source_group,
        device_or_unknown,
        played_at_month_decile,
        device_or_unknown_avg_played_at_month_decile,
        played_at_month_decile_cume_dist,
        last_known_played_at_month,
        TO_DATE(played_at_month) AS played_at_month_date,
        COALESCE(device_or_unknown, 'UNKNOWN') AS device_or_unknown_or_unknown
    FROM ranked_2
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT played_at_month_decile, source_group, device_or_unknown_avg_played_at_month_decile
    FROM tagged
) src
PIVOT (
    MAX(device_or_unknown_avg_played_at_month_decile)
    FOR source_group IN ('other', 'group_b', 'group_a')
)
