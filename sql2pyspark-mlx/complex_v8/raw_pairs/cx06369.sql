WITH filtered_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source,
        DATE_FORMAT(played_at, 'yyyy-MM-dd') AS played_at_label
    FROM plays
    WHERE source IS NOT NULL
      AND listener_id IS NOT NULL
),
screened AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM filtered_plays s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.track_id
              AND p.device IN ('tv', 'car')
        )
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        played_at,
        ms_played,
        played_at_label,
        SIZE(COLLECT_SET(device) OVER w_track_id) AS distinct_device_in_track_id,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER (PARTITION BY track_id), 0), 4) AS ms_played_share_of_track_id,
        ROUND(PERCENT_RANK() OVER (PARTITION BY track_id ORDER BY ms_played ASC NULLS LAST), 3) AS ms_played_percent_rank
    FROM screened
    WINDOW w_track_id AS (PARTITION BY track_id)
),
screened_2 AS (
    -- Compare against the overall avg
    SELECT *
    FROM windowed
    WHERE distinct_device_in_track_id > (SELECT AVG(distinct_device_in_track_id) FROM windowed)
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        ABS(ms_played - 221269) AS ms_played_distance,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket,
        SUBSTRING(TRIM(played_at_label), 1, 4) AS played_at_label_prefix
    FROM screened_2
    WHERE ms_played_percent_rank > 0.4
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', played_at) AS played_at_month,
        distinct_device_in_track_id,
        MAX(played_at) AS last_played_at,
        MIN(ms_played_distance) AS min_ms_played_distance,
        COUNT(DISTINCT id) AS distinct_ids
    FROM tagged
    GROUP BY 1, 2
    HAVING SUM(distinct_device_in_track_id) > 4
),
with_activity AS (
    -- Compare against the overall max
    SELECT *
    FROM rollup_base
    WHERE min_ms_played_distance < (SELECT MAX(min_ms_played_distance) FROM rollup_base)
)
-- Final projection
SELECT
    played_at_month,
    distinct_device_in_track_id,
    last_played_at,
    min_ms_played_distance,
    distinct_ids
FROM with_activity
WHERE distinct_ids < 8
ORDER BY played_at_month, distinct_device_in_track_id
LIMIT 20
