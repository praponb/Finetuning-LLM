WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(duration_sec * 100, 2) AS adjusted_duration_sec
    FROM web_events
    WHERE event_time >= DATE '2023-07-11'
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM cleaned
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM flagged
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY event_type ORDER BY event_time DESC) AS row_num
    FROM islands
    QUALIFY row_num = 1
),
with_dims AS (
    -- Attach sessions attributes
    SELECT
        e.*,
        p.device,
        p.country
    FROM ranked e
    LEFT JOIN sessions p
        ON e.session_id = p.id
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('QUARTER', event_time) AS quarter_start,
    SUM(CASE WHEN page = '/checkout' THEN duration_sec ELSE 0 END) AS checkout_duration_sec,
    ROUND(SUM(session_id_2) / NULLIF(COUNT(CASE WHEN page = '/search' THEN 1 END), 0), 2) AS session_id_2_per_search,
    BIT_OR(duration_sec) AS duration_sec_bit_or,
    COUNT(*) FILTER (WHERE page = '/product') AS product_rows
FROM with_dims
GROUP BY DATE_TRUNC('QUARTER', event_time)
HAVING checkout_duration_sec < 1000000
ORDER BY checkout_duration_sec DESC NULLS FIRST;
