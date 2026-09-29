WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        CASE WHEN page = '/cart' THEN 1 ELSE 0 END AS is_cart
    FROM web_events
    WHERE event_time >= DATE '2023-09-16'
      AND page IS DISTINCT FROM '/checkout'
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM filtered
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id, session_id
    FROM sessionized
    INTERSECT ALL
    SELECT id, session_id
    FROM web_events
    WHERE session_id = 27
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM sessionized s
    INNER JOIN kept_keys k ON s.id = k.id AND s.session_id = k.session_id
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY event_type, session_id ORDER BY session_start_flag DESC NULLS FIRST) AS session_start_flag_dense_rank,
        MAX(is_cart) OVER (PARTITION BY session_id ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS is_cart_max_7d
    FROM verified
)
-- Join a dimension and aggregate
SELECT
    d.browser,
    src.page,
    COUNT(*) AS rows_total,
    MAX(src.is_cart_max_7d) AS max_is_cart_max_7d,
    GROUPING_ID() AS grouping_level
FROM metrics src
LEFT JOIN sessions d ON src.session_id = d.id
GROUP BY GROUPING SETS ((d.browser, src.page), (d.browser), ())
ORDER BY max_is_cart_max_7d NULLS LAST;
