WITH with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(started_at) OVER (PARTITION BY device ORDER BY started_at ASC, id ASC) AS prev_started_at
    FROM sessions
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_started_at IS NULL OR started_at > prev_started_at + INTERVAL '30' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM with_prev
),
sessions_sessions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY device ORDER BY started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at ASC, id ASC) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at ASC, id ASC) + INTERVAL '14' DAY THEN 1 ELSE 0 END AS is_new_burst_2
    FROM sessions_sessions
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst_2) OVER (PARTITION BY browser ORDER BY started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
flagged_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN prev_started_at - LAG(prev_started_at) OVER (PARTITION BY device ORDER BY prev_started_at ASC, id ASC) > INTERVAL 90 DAYS THEN 1 ELSE 0 END AS is_new_burst_3
    FROM sessionized
),
sessionized_sessions AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        customer_id,
        device,
        started_at,
        country,
        prev_started_at,
        burst_id,
        is_new_burst_2,
        session_no,
        SUM(is_new_burst_3) OVER (PARTITION BY device ORDER BY prev_started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no_2
    FROM flagged_2
),
with_last AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.web_event_page,
        x.event_type
    FROM sessionized_sessions s
    JOIN LATERAL (
            SELECT c.page AS web_event_page, c.event_type
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY web_event_page ORDER BY session_no DESC, id ASC) AS rn
    FROM with_last
    QUALIFY rn = 1
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT country, customer_id
    FROM ranked
    INTERSECT ALL
    SELECT country, customer_id
    FROM sessions
    WHERE country IN ('US', 'UK')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM ranked s
    INNER JOIN remaining_keys k ON s.country = k.country AND s.customer_id = k.customer_id
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        device,
        burst_id,
        session_no,
        session_no_2,
        rn,
        DENSE_RANK() OVER (PARTITION BY country, customer_id ORDER BY session_no DESC) AS session_no_dense_rank,
        MAX(is_new_burst_2) OVER (PARTITION BY event_type) AS event_type_max_is_new_burst_2,
        COUNT(burst_id) OVER (PARTITION BY customer_id ORDER BY CAST(started_at AS BIGINT) ASC RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS burst_id_count_7d,
        RANK() OVER (ORDER BY burst_id ASC) AS burst_id_rank,
        AVG(session_no) OVER (PARTITION BY device) AS device_avg_session_no
    FROM kept_rows
    WHERE is_new_burst_2 = 1
)
-- Aggregate with conditional metrics
SELECT
    burst_id,
    SIZE(COLLECT_SET(device)) AS num_devices,
    COUNT(*) AS event_count
FROM scored
GROUP BY CUBE (burst_id)
HAVING SIZE(COLLECT_SET(device)) > 1
