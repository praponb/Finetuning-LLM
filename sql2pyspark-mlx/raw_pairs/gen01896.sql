WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LOWER(country) AS country_lower
    FROM sessions
    WHERE started_at > TIMESTAMP '2023-09-08 08:00:00'
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(started_at) OVER (PARTITION BY device ORDER BY started_at, id) AS prev_started_at
    FROM filtered
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_started_at IS NULL OR started_at > prev_started_at + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM prev_events
),
sessions_sessions AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        SUM(is_new_session) OVER (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
flagged_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM sessions_sessions
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged_2
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM islands s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.event_time <= s.started_at
        )
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        src.*,
        p.signup_date,
        p.segment,
        p.age
    FROM kept src
    LEFT JOIN customers p
        ON src.customer_id = p.id
),
pivoted AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            browser,
            device,
            session_id
        FROM with_dims
    ) src
    PIVOT (
        MAX(session_id) AS total,
        COUNT(1) AS cnt
        FOR device IN ('mobile', 'tablet')
    )
)
SELECT browser, mobile_total
FROM pivoted
ORDER BY mobile_total;
