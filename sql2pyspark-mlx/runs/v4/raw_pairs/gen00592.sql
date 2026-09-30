WITH enriched AS (
    SELECT
        *,
        ROW_NUMBER() OVER w_grp seq_no,
        ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at ASC, id ASC) row_seq
    FROM sessions
    WINDOW w_grp AS (PARTITION BY country ORDER BY started_at ASC, id ASC)
),
with_last AS (
    SELECT
        s.*,
        top1.duration_sec,
        top1.event_type
    FROM enriched s
    JOIN LATERAL (
            SELECT c.duration_sec, c.event_type
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
filtered AS (
    SELECT
        id,
        customer_id,
        device,
        started_at,
        country,
        seq_no,
        duration_sec,
        event_type,
        ROUND(duration_sec, 2) duration_sec_rounded,
        LOWER(event_type) event_type_lower
    FROM with_last
    WHERE started_at > TIMESTAMP '2023-09-05 07:00:00'
      AND event_type IS NOT NULL
),
enriched_sessions AS (
    SELECT
        *,
        COUNT(duration_sec) OVER (PARTITION BY customer_id, device) customer_id_count_duration_sec,
        SUM(duration_sec) OVER (PARTITION BY event_type ORDER BY duration_sec_rounded ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_duration_sec,
        SUM(duration_sec) OVER (PARTITION BY country ORDER BY duration_sec ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_duration_sec_2
    FROM filtered
),
summary AS (
    SELECT
        customer_id,
        MAX(running_duration_sec_2) FILTER (WHERE device = 'mobile') AS mobile_max_running_duration_sec_2,
        SORT_ARRAY(COLLECT_SET(event_type)) AS event_type_list,
        APPROX_PERCENTILE(duration_sec, 0.5) AS duration_sec_p50,
        COUNT(*) FILTER (WHERE device = 'mobile') AS mobile_rows,
        SIZE(COLLECT_SET(event_type)) AS num_event_types
    FROM enriched_sessions
    GROUP BY customer_id
    HAVING COUNT(*) FILTER (WHERE device = 'mobile') > 1
)
SELECT
    customer_id,
    mobile_max_running_duration_sec_2,
    event_type_list,
    duration_sec_p50,
    mobile_rows
FROM summary
