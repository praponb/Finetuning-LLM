WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        browser,
        started_at,
        country,
        SUBSTRING(browser, 1, 2) AS browser_prefix
    FROM sessions
    WHERE device = 'mobile'
),
with_latest AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.web_event_event_type,
        x.web_event_event_time
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.event_type AS web_event_event_type, c.event_time AS web_event_event_time
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time <= s.started_at
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.event_time <= s.web_event_event_time
        )
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(started_at) OVER (PARTITION BY web_event_event_type ORDER BY started_at, id) AS prev_started_at
    FROM eligible
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_started_at IS NULL OR started_at > prev_started_at + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM prev_events
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        customer_id,
        started_at,
        country,
        web_event_event_type,
        web_event_event_time,
        SUM(is_new_burst) OVER (PARTITION BY web_event_event_type ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
),
enriched AS (
    -- Attach customers attributes
    SELECT
        e.*,
        p.segment,
        p.city
    FROM sessionized e
    JOIN customers p
        ON e.customer_id = p.id
    WHERE p.credit_limit < 6400
)
-- Join a dimension and aggregate
SELECT
    dim.city AS customer_city,
    dim.segment AS customer_segment,
    COUNT(*) AS rows_total,
    MAX(e.session_id) AS max_session_id,
    GROUPING_ID() AS gid
FROM enriched e
JOIN customers dim ON e.customer_id = dim.id
GROUP BY CUBE (dim.city, dim.segment);
