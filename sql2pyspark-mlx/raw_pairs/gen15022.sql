WITH flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) > INTERVAL 14 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM sessions
),
sessions_sessions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
flagged_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) > INTERVAL 90 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM sessions_sessions
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged_2
),
latest_match AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.event_time,
        latest.web_event_page
    FROM islands s
    JOIN LATERAL (
            SELECT c.event_time, c.page AS web_event_page
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time <= s.started_at
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
joined AS (
    -- Attach customers attributes
    SELECT
        s.*,
        c.age,
        c.credit_limit,
        c.signup_date
    FROM latest_match s
    JOIN customers c
        ON s.customer_id <=> c.id
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY country ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY country ORDER BY event_time, id) + INTERVAL '7' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM joined
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY country ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id_2
    FROM gaps
)
-- Multi-level aggregation
SELECT
    CASE WHEN credit_limit >= 17000 THEN 'PREMIUM' ELSE 'STANDARD' END AS credit_limit_tier,
    DATE_TRUNC('QUARTER', event_time) AS quarter_start,
    COUNT(DISTINCT web_event_page) AS distinct_web_event_pages,
    MAX(started_at) AS last_seen,
    APPROX_PERCENTILE(session_start_flag, ARRAY(0.25, 0.5, 0.75)) AS session_start_flag_quartiles,
    GROUPING(CASE WHEN credit_limit >= 17000 THEN 'PREMIUM' ELSE 'STANDARD' END) AS grouping_id
FROM sessionized
GROUP BY CUBE (CASE WHEN credit_limit >= 17000 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('QUARTER', event_time));
