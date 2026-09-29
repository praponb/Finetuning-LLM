WITH enriched AS (
    -- Attach customers attributes
    SELECT
        a.id,
        a.customer_id,
        a.device,
        a.browser,
        a.started_at,
        a.country,
        d.segment
    FROM sessions a
    LEFT JOIN customers d
        ON a.customer_id = d.id
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(started_at) OVER (PARTITION BY device ORDER BY started_at, id) AS prev_started_at
    FROM enriched
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_started_at IS NULL OR started_at > prev_started_at + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM with_prev
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        device,
        started_at,
        country,
        segment,
        SUM(session_start_flag) OVER (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM islands
    QUALIFY RANK() OVER (PARTITION BY device ORDER BY session_no DESC) = 1
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id) > INTERVAL '60' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM ranked
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        device,
        started_at,
        country,
        session_no,
        SUM(session_start_flag) OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        started_at,
        session_no,
        burst_id,
        MIN(session_no) OVER (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_session_no,
        SUM(session_no) OVER (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_session_no,
        AVG(session_no) OVER (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_session_no,
        SUM(session_no) OVER (PARTITION BY device ORDER BY burst_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_session_no
    FROM sessionized
)
SELECT
    id,
    session_no,
    burst_id,
    last4_min_session_no,
    last4_avg_session_no,
    running_session_no
FROM metrics
ORDER BY session_no DESC;
