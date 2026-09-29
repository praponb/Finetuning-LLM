WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        LENGTH(event_type) AS event_type_length
    FROM web_events
    WHERE event_time >= DATE '2023-04-14'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        event_type_length,
        NTILE(10) OVER (PARTITION BY page ORDER BY event_type_length DESC, id) AS event_type_length_bucket,
        MIN(duration_sec) OVER (PARTITION BY session_id) AS session_id_min_duration_sec,
        AVG(event_type_length) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS event_type_length_avg_6_prior
    FROM cleaned
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM scored
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        event_type,
        page,
        event_time,
        event_type_length_avg_6_prior,
        SUM(is_new_burst) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM flagged
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_type_length_avg_6_prior DESC, id) AS event_seq,
        STDDEV_SAMP(event_type_length_avg_6_prior) OVER (PARTITION BY page, event_type) AS page_stddev_samp_event_type_length_avg_6_prior
    FROM sessionized
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        event_type,
        SORT_ARRAY(COLLECT_SET(event_type)) AS event_type_list,
        ROUND(AVG(session_id_2), 2) AS avg_session_id_2
    FROM with_windows
    GROUP BY 1
)
SELECT
    event_type,
    avg_session_id_2,
    CASE WHEN event_type RLIKE '^(add_to_cart|click)$' THEN 'EXTENDED' WHEN event_type RLIKE '^(page_view|purchase)$' THEN 'OTHER_ZONE' ELSE 'PRIORITY' END AS event_type_zone
FROM stats
ORDER BY event_type_zone
