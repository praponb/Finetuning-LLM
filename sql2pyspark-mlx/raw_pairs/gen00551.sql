WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        session_id,
        event_type,
        duration_sec,
        HOUR(event_time) AS event_time_hour
    FROM web_events
    WHERE event_type IN ('add_to_cart', 'purchase', 'click')
      AND page IS NOT DISTINCT FROM '/checkout'
),
remaining_keys AS (
    -- Set difference on keys
    SELECT event_type, id
    FROM base
    EXCEPT ALL
    SELECT event_type, id
    FROM web_events
    WHERE session_id IN (7, 13, 28)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    JOIN remaining_keys k ON s.event_type = k.event_type AND s.id = k.id
),
with_dims AS (
    -- Attach sessions attributes
    SELECT
        src.*,
        d.device,
        d.browser,
        d.country
    FROM kept_rows src
    LEFT JOIN sessions d
        ON src.session_id = d.id
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        session_id,
        event_type,
        duration_sec,
        event_time_hour,
        device,
        country,
        CUME_DIST() OVER (ORDER BY event_time_hour NULLS FIRST) AS event_time_hour_cume_dist,
        AVG(event_time_hour) OVER (PARTITION BY session_id) AS session_id_avg_event_time_hour,
        MAX(duration_sec) OVER (PARTITION BY device) AS device_max_duration_sec,
        RANK() OVER (ORDER BY duration_sec DESC) AS duration_sec_rank
    FROM with_dims
    WHERE event_type <> 'search'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        duration_sec,
        event_time_hour,
        device,
        session_id_avg_event_time_hour,
        device_max_duration_sec,
        duration_sec_rank,
        PERCENT_RANK() OVER (PARTITION BY device ORDER BY device_max_duration_sec DESC) AS device_max_duration_sec_percent_rank,
        MIN(duration_sec) OVER (PARTITION BY session_id) AS session_id_min_duration_sec
    FROM scored
),
scored_web_events AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        event_time_hour,
        session_id_avg_event_time_hour,
        device_max_duration_sec,
        duration_sec_rank,
        session_id_min_duration_sec,
        PERCENT_RANK() OVER w_grp AS event_time_hour_percent_rank,
        RANK() OVER w_grp_2 AS session_id_avg_event_time_hour_rank,
        RANK() OVER (PARTITION BY device ORDER BY session_id_min_duration_sec DESC NULLS LAST) AS session_id_min_duration_sec_rank,
        RANK() OVER w_grp_2 AS session_id_avg_event_time_hour_rank_2,
        MAX(duration_sec) OVER (PARTITION BY device) AS device_max_duration_sec_2
    FROM with_windows
    WINDOW w_grp AS (ORDER BY event_time_hour DESC NULLS FIRST), w_grp_2 AS (PARTITION BY device ORDER BY session_id_avg_event_time_hour DESC)
)
-- Multi-level aggregation
SELECT
    CASE WHEN event_time_hour >= 68 THEN 'PREMIUM' ELSE 'STANDARD' END AS event_time_hour_tier,
    CASE WHEN duration_sec_rank >= 6 THEN 'LARGE' ELSE 'SMALL' END AS duration_sec_rank_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT id) AS distinct_ids_2,
    ROUND(AVG(session_id_min_duration_sec_rank), 2) AS avg_session_id_min_duration_sec_rank,
    GROUPING_ID() AS gid
FROM scored_web_events
GROUP BY GROUPING SETS ((CASE WHEN event_time_hour >= 68 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN duration_sec_rank >= 6 THEN 'LARGE' ELSE 'SMALL' END), (CASE WHEN event_time_hour >= 68 THEN 'PREMIUM' ELSE 'STANDARD' END));
