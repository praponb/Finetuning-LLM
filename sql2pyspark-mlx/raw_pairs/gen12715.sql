WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        INITCAP(TRIM(event_type)) AS event_type_title,
        CASE WHEN event_type IN ('add_to_cart', 'purchase') THEN 'Group A' ELSE 'Other' END AS event_type_group
    FROM web_events
    WHERE event_time > TIMESTAMP '2023-03-14 05:00:00'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(duration_sec, 1) OVER (ORDER BY duration_sec, id) AS prev_duration_sec,
        SUM(duration_sec) OVER (PARTITION BY YEAR(event_time) ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_duration_sec,
        LEAD(duration_sec, 1) OVER (PARTITION BY TO_DATE(event_time), session_id ORDER BY event_time, id) AS next_duration_sec
    FROM filtered
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        session_id,
        page,
        event_time,
        duration_sec,
        event_type_title,
        event_type_group,
        prev_duration_sec,
        cumulative_duration_sec,
        next_duration_sec
    FROM (
        SELECT *, RANK() OVER (PARTITION BY event_type ORDER BY event_time NULLS LAST) AS latest_rank
        FROM enriched
    ) t
    WHERE latest_rank = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(cumulative_duration_sec) OVER (PARTITION BY DATE_TRUNC('MONTH', event_time), page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cumulative_duration_sec,
        AVG(cumulative_duration_sec) OVER (PARTITION BY session_id ORDER BY duration_sec, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS cumulative_duration_sec_avg_2,
        ROW_NUMBER() OVER (PARTITION BY YEAR(event_time), session_id ORDER BY cumulative_duration_sec, id) AS row_seq
    FROM ranked
    WHERE duration_sec != 277
),
with_dims AS (
    -- Attach sessions attributes
    SELECT
        e.id,
        e.session_id,
        e.event_time,
        e.event_type_group,
        e.next_duration_sec,
        e.cumulative_duration_sec_avg_2,
        e.row_seq,
        d.device
    FROM metrics e
    LEFT JOIN sessions d
        ON e.session_id = d.id
),
ranked_web_events AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        event_time,
        next_duration_sec,
        cumulative_duration_sec_avg_2,
        row_seq,
        device
    FROM (
        SELECT *, RANK() OVER (PARTITION BY session_id ORDER BY cumulative_duration_sec_avg_2 DESC NULLS FIRST) AS rn
        FROM with_dims
    ) t
    WHERE rn = 1
)
-- Multi-level aggregation
SELECT
    device,
    SORT_ARRAY(COLLECT_SET(device)) AS device_list,
    SIZE(COLLECT_SET(device)) AS num_devices,
    APPROX_PERCENTILE(next_duration_sec, ARRAY(0.25, 0.5, 0.75)) AS next_duration_sec_quartiles
FROM ranked_web_events
GROUP BY device;
